require './user_brick'
require 'rspec'
require 'pp'

describe GoodData::Bricks::UserBrick do

# adding user to domain, count of domain
# Add a user from a domain to the project
# Checking role change

  it 'should change the default column from role to privileges' do

    GoodData.logging_on

    GoodData.connect('svarovsky+gem_tester@gooddata.com','jindrisska')
    #GoodData.connection.cookies[:cookies]['GDCAuthSST']

    project = GoodData::Project['tzk6o6t45ku3u875ttbebv1avjxppu75']
    GoodData.connect('svarovsky+gem_tester@gooddata.com', 'jindrisska')


    params = {
        :domain => "gooddata-tomas-svarovsky",
        :project => "pua4g8eplv06iayn5nkz0brqnb5l7i70",
        :csv_path => "./demo.csv"
    }

    # Add a demo user - ++ TODO: Add the clean up :before/:after

    username = (0...8).map { (65 + rand(26)).chr }.join

    File.open("demo.csv", "w") { |f|
      f.puts "email,login,first_name,last_name,role,password\n#{username}1@gooddata.com,#{username}1@gooddata.com,Paul,Columny,adminRole,services\nsvarovsky+gem_tester@gooddata.com,svarovsky+gem_tester@gooddata.com,Tomas,Svarovsky,adminRole,jindrisska\n#{username}@gooddata.com,#{username}@gooddata.com,John,Smith,viewerRole,goodexample1\n"
    }
    open('./demo.csv', 'a') { |f|
      f.puts "#{username}443@gooddata.com,#{username}342@gooddata.com,James,Smith,viewerRole,goodexample1"
    }

    user_count_before = project.users.length

    user_brick = GoodData::Bricks::UserBrick.new()

    p = GoodData::Bricks::Pipeline.prepare([user_brick])

    params = {
        "domain" => "gooddata-tomas-svarovsky",
        "gdc_project" => project,
        "csv_path" => "./demo.csv"
    }

    p.call(params)

    user_count_after = project.users.length

    # Clean up the demo file
    File.open("demo.csv", "w") { |f|
      f.puts "email,login,first_name,last_name,role,password\nexample+identity+2@gooddata.com,example+identity+2@gooddata.com,Paul,Columny,viewerRole,services\nsvarovsky+gem_tester@gooddata.com,svarovsky+gem_tester@gooddata.com,Tomas,Svarovsky,adminRole,jindrisska\n\n#{username}@gooddata.com,#{username}@gooddata.com,John,Smith,viewerRole,goodexample1"
    }

    fail 'Brick did not add users to project' unless user_count_after > user_count_before

  end



end
