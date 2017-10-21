# -*- coding: utf-8 -*-

module Embulk
  module Input
    module HealthplanetApi
      class Column

        def initialize(lang)
          case lang.downcase
          when 'ja', 'japanese'
            @names = {
              :time           => '測定日時',
              :model          => 'モデル',
              :sbp            => '最高血圧',
              :dbp            => '最低血圧',
              :pulse          => '脈拍',
            }
          when 'en', 'english'
            @names = {
              :time           => 'time',
              :model          => 'model',
              :sbp            => 'Systolic Blood Pressure',
              :dbp            => 'Diastolic Blood Pressure',
              :pulse          => 'Pulse',
            }
          else
            # returns as-is API tag
            @names = {
              :time           => 'time',
              :model          => 'model',
              :weight         => '622E',
              :body_fat       => '622F',
              :muscle_mass    => '6230',
            }
          end
        end

        def name(key)
          @names[key]
        end
      end
    end
  end
end
