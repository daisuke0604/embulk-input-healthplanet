require 'faraday'
require 'faraday-cookie_jar'
require 'oga'
require 'nkf'
require 'json'
require 'time'
require 'embulk/input/healthplanet_api/column'

module Embulk
  module Input

    class Healthplanet < InputPlugin
      Plugin.register_input("healthplanet", self)

      # Default redirect URI for Client Application
      REDIRECT_URI = 'https://www.healthplanet.jp/success.html'

      # Default scope
      DEFAULT_SCOPE = 'sphygmomanometer'

      # Default response type
      DEFAULT_RESPONSE_TYPE = 'code'

      # Default grant type
      DEFAULT_GRANT_TYPE = 'authorization_code'

      # All tags for innerscan
      ALL_TAGS = '622E,622F,6230'

      # Health Planet API can response only in 3 months
      RESPONSE_INTERVAL = 60*60*24*30*3

      # embulk-input-healthplanet retrieves data from one year ago by default
      # If you need data more than one year ago, please set 'next_from' parameter
      DEFAULT_FROM_TIME = 60*60*24*365

      def self.transaction(config, &control)
        # configuration code:
        task = {
          # Account for Health Planet
          'login_id' => config.param('login_id', :string),
          'password' => config.param('password', :string),
          # Credential for embulk-input-healthplanet, application type "Client Application"
          'client_id' => config.param('client_id', :string),
          'client_secret' => config.param('client_secret', :string),
          # This plugin retrieves new data after this time
          'next_from' => config.param('next_from', :string, :default => nil)
        }

        lang = config.param('lang', :string, :default => 'en')
        col = HealthplanetApi::Column.new(lang)

        columns = [
          Column.new(0, col.name(:time), :timestamp),
          Column.new(1, col.name(:model), :string),
          Column.new(2, col.name(:sbp), :long),
          Column.new(3, col.name(:dbp), :long),
          Column.new(4, col.name(:pulse), :long),
        ]

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = task_reports.first
        return next_config_diff
      end

      def init
        login_id = task['login_id']
        password = task['password']
        client_id = task['client_id']
        client_secret = task['client_secret']
        if task['next_from']
          @next_from = Time.strptime(task['next_from'], '%Y-%m-%d %H:%M:%S')
        end

        # Setup connection
        @conn = Faraday.new(:url => 'https://www.healthplanet.jp') do |faraday|
          faraday.request  :url_encoded
          faraday.response :logger
          faraday.use      :cookie_jar
          faraday.adapter  Faraday.default_adapter
        end

        # Request Authentication page: /oauth/auth
        response = @conn.get do |req|
          req.url '/oauth/auth'
          req.params[:client_id]     = client_id
          req.params[:redirect_uri]  = REDIRECT_URI
          req.params[:scope]         = DEFAULT_SCOPE
          req.params[:response_type] = DEFAULT_RESPONSE_TYPE
        end

        # Login and set session information
        response = @conn.post 'login_oauth.do', { :loginId => login_id, :passwd => password, :send => '1', :url => "https://www.healthplanet.jp/oauth/auth?client_id=#{client_id}&redirect_uri=#{REDIRECT_URI}&scope=#{DEFAULT_SCOPE}&response_type=#{DEFAULT_RESPONSE_TYPE}" }

        unless response.status == 302
          # TODO return error in Embulk manner
          print "Login failure\n"
        end

        # Get auth page again with JSESSIONID
        response = @conn.get do |req|
          req.url '/oauth/auth'
          req.params[:client_id]     = client_id
          req.params[:redirect_uri]  = REDIRECT_URI
          req.params[:scope]         = DEFAULT_SCOPE
          req.params[:response_type] = DEFAULT_RESPONSE_TYPE
        end

        # Read oauth_token
        document = Oga.parse_html(NKF.nkf('-Sw', response.body))
        oauth_token = document.at_xpath('//input[@name="oauth_token"]').get('value')

        # Post /oauth/approval.do
        response = @conn.post '/oauth/approval.do', { :approval => 'true', :oauth_token => oauth_token }

        # Read code
        document = Oga.parse_html(NKF.nkf('-Sw', response.body))
        code = document.at_xpath('//textarea[@id="code"]').text

        # Get request token
        response = @conn.post do |req|
          req.url '/oauth/token'
          req.params[:client_id]     = client_id
          req.params[:client_secret] = client_secret
          req.params[:redirect_uri]  = REDIRECT_URI
          req.params[:code]          = code
          req.params[:grant_type]    = DEFAULT_GRANT_TYPE
        end

        tokens = JSON.parse(response.body)
        @access_token = tokens['access_token']
      end

      def run
        from = @next_from.nil? ? (Time.now - DEFAULT_FROM_TIME) : @next_from
        last_date = nil

        while from < Time.now
          to = from + RESPONSE_INTERVAL
          date = innerscan(from, to)
          # Update last_date if any data exists
          last_date = date if date

          # Next request must start from 1 minute later to avoid redundant data
          from = to + 60
        end

        page_builder.finish

        task_report = {}
        unless preview? or last_date.nil?
          # Next request must start from 1 minute later to avoid redundant data
          task_report = { :next_from => (last_date + 60).strftime('%Y-%m-%d %H:%M:%S') }
        end
        return task_report
      end

      def innerscan(from = nil, to = nil)
        response = @conn.get do |req|
          req.url 'status/sphygmomanometer.json'
          req.params[:access_token] = @access_token
          # 0: registered time, 1: measured time
          req.params[:date] = 1
          req.params[:from] = from.strftime('%Y%m%d%H%M%S') unless from.nil?
          req.params[:to]   = to.strftime('%Y%m%d%H%M%S')   unless to.nil?
          req.params[:tag]  = ALL_TAGS
        end

        data = JSON.parse(response.body)

        result = {}

        data['data'].each do |record|
          date = Time.strptime(record['date'], '%Y%m%d%H%M')

          result[date] ||= {}
          result[date]['model'] ||= record['model']
          result[date][record['tag']]  = record['keydata']
        end

        dates = result.keys.sort
        last_date = dates.last

        dates.each do |date|
          page = Array.new(5)
          page[0] = date
          result[date].each do |key, value|
            case key
            when 'model'
              page[1] = value
            when '622E'
              # SBP
              page[2] = value
            when '622F'
              # DBP
              page[3] = value
            when '6230'
              # Pulse
              page[4] = value
            end
          end

          page_builder.add(page)
        end

        last_date
      end

      def preview?
        begin
          org.embulk.spi.Exec.isPreview()
        rescue java.lang.NullPointerException => e
          false
        end
      end
    end
  end
end
