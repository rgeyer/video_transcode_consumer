# Copyright (c) 2012 Ryan J. Geyer
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'fog'
require 'json'
require 'bunny'
require 'tmpdir'
require 'net/http'
require 'logger'
require 'digest/md5'
require 'posix/spawn'

module RGeyer
  class TranscodeConsumer

    class DownloadError < Exception; end
    class TranscodeError < Exception; end
    class UploadError < Exception; end

    # Initializes a transcode_consumer object
    #
    # === Parameters
    # amqp_hostname(String):: The hostname of the AMQP server hosting input, output, and error queues
    # gstore_bucket_name(String):: The name of the Google Storage bucket which will be used to store the transcoded videos
    # options(Hash):: A hash of options.  Possible options are;
    #   :input_queue(String) - The name of the AMQP input queue
    #   :output_queue(String) - The name of the AMQP output queue
    #   :error_queue(String) - The name of the AMQP error queue
    #   Any option available to the bunny client initializer : https://github.com/ruby-amqp/bunny/blob/master/lib/bunny/client.rb#L8
    def initialize(amqp_hostname, gstore_bucket_name, options={})
      options.merge!({:host => amqp_hostname})
      @input_queue_name = options[:input_queue_name] || 'encode_input'
      @output_queue_name = options[:output_queue_name] || 'encode_output'
      @error_queue_name = options[:error_queue_name] || 'encode_error'

      @logger = options[:logger] || Logger.new("/var/log/transode_consumer-#{$$}.log")

      @gstore_bucket_name = gstore_bucket_name

      @gstore = Fog::Storage.new({:provider => 'Google'})
      @bunny = Bunny.new(options)
      @bunny.start
      @input_queue  = @bunny.queue(@input_queue_name)
      @output_queue = @bunny.queue(@output_queue_name)
      @error_queue  = @bunny.queue(@error_queue_name)

      @output_exchange  = @bunny.exchange('')
      @error_exchange   = @bunny.exchange('')
    end

    # Pops a single transcoding job off of the input queue and processes it, populating the
    # output and error queues as appropriate
    #
    # Raises nothing.  If it's successful, the result will be in the Google Storage
    # bucket and the output queue.  If it's unsuccessful, the result will be in the error queue
    #
    # === Return
    # result(bool):: True if a message existed in the queue and was popped off and processed.  False if the queue was empty
    def do_job
      message = @input_queue.pop
      return false if message[:payload].to_s == 'queue_empty'
      job = JSON.parse(message[:payload])
      if job['type'] == 'rss' && job['object']['media_content_url']
        exceptions = []
        source_tmpfile = get_temp_filename
        dest_tmpfile = get_temp_filename "#{Digest::MD5.hexdigest(job['object']['media_content_url'])}.mp4"
        begin
          @logger.debug("Beginning download of #{job['object']['media_content_url']} to #{source_tmpfile}")
          download_file job['object']['media_content_url'], source_tmpfile
        rescue DownloadError => e
          exceptions << e
          #publish_to_error_queue job, e
          return true
        end
        job['handbrake_presets'].each do |handbrake_preset|
          @logger.info("Transcoding #{job['object']['media_content_url']} with HandBrake preset #{handbrake_preset}")
          begin
            transcode_file source_tmpfile, dest_tmpfile, handbrake_preset
            upload_file "#{job['object']['media_title']}/#{handbrake_preset}.mp4", dest_tmpfile
            File.delete(dest_tmpfile) if File.exist? dest_tmpfile
          rescue Exception => e
            #publish_to_error_queue job, e
            exceptions << e
          end
        end
        if exceptions.length > 0
          publish_to_error_queue job, exceptions
        else
          File.delete(source_tmpfile) if File.exist? source_tmpfile
          publish_to_output_queue job, @gstore_bucket_name
        end
      end
      true
    end

    # Returns some basic information about the input queue
    #
    # === Return
    # A hash containing the message count and consumer count of the input queue
    def get_input_queue_status
      @input_queue.status
    end

    private

    # Puts an error message in the error queue
    #
    # === Parameters
    # input_job(Hash):: The original job object
    # exception(Exception):: The exception object that caused the error
    #
    def publish_to_error_queue(input_job, exception)
      @logger.error(exception)
      begin
        @error_exchange.publish(
          {:input_job => input_job, :exception => exception}.to_json,
          :key => @error_queue_name
        )
      rescue Excon::Error => e
        message = <<-EOF
An error occurred while attempting to add message to error queue.

---------- Message ----------
#{{:input_job => input_job, :exception => exception}.to_json}

----------- Error -----------
#{e.inspect}
EOF

        @logger.error(message)
      end
    end

    # Puts an output message in the output queue
    #
    # === Parameters
    # input_job(Hash):: The original job object
    # gstore_bucket_name(String):: The name of the Google Storage bucket that contains the completed job
    #
    def publish_to_output_queue(input_job, gstore_bucket_name)
      @output_exchange.publish(
        {:input_job => input_job, :gstore_bucket_name => gstore_bucket_name}.to_json,
        :key => @output_queue_name
      )
    end

    # Downloads a file to a specified location, following 302 redirects
    #
    # === Parameters
    # source_uri(String):: A URI to the file which should be downloaded
    # dest_filepath(String):: The full filepath where the URI should be downloaded
    # redirect_count(int):: Only used when this method recurses.  Used to indicate how many redirects have been followed
    #
    # === Raise
    # DownloadError:: on failure to download the file
    def download_file(source_uri, dest_filepath, redirect_count=0)
      begin
        uri = URI(source_uri)
      rescue URI::InvalidURIError => e
        raise DownloadError.new("#{source_uri} is not a valid download URI")
      end
      Net::HTTP.start(uri.host, uri.port) { |http|
        resp = http.get(uri.request_uri)
        # TODO: Allow the number of redirects to be set as an option when instantiating this class
        case resp.code
          when "302"
            if redirect_count > 1
              raise DownloadError.new("Redirected too many times attempting to download.  #{redirect_count} redirects were followed before giving up")
            end
            @logger.debug("Download response was 302.  Following redirect to #{resp.header['location'].first}, having already followed #{redirect_count} redirects.")
            download_file(resp.header['location'].first, dest_filepath, redirect_count + 1)
          when "200"
            @logger.debug("Download response was 200, downloading body with a length of #{resp.body.length}")
            File.open(dest_filepath, "wb") { |file|
              file.write(resp.body)
            }
          else
            raise DownloadError.new("Download request failed with #{resp.code} #{resp.inspect}")
        end
      }
    end

    def get_temp_filename(filename=nil)
      File.join(Dir.tmpdir, filename || "#{Time.now.to_i}.mp4")
    end

    # Executes HandBrakeCLI on the commandline to transcode a source file into a destination file with a given preset
    #
    # === Parameters
    # source_file(String):: The full path to the source file to transcode
    # dest_file(String):: The full filepath for the destination (transcoded) file
    # handbrake_preset(String):: The name of a valid HandBrake preset to use for transcoding
    #
    # === Raise
    # TranscodeError:: on any handbrake failure
    #
    def transcode_file(source_file, dest_file, handbrake_preset)
      raise TranscodeError.new("HandBrakeCLI is not in the current path, maybe it's not installed?") unless `which HandBrakeCLI` =~ /HandBrakeCLI/
      raise TranscodeError.new("Source file #{source_file} not found.") unless File.exist? source_file
      child = blocking_popen "HandBrakeCLI -i \"#{source_file}\" -o \"#{dest_file}\" --preset=\"#{handbrake_preset}\""

      debug_message = <<EOF
HandBrakeCLI exitted with #{child.status.exitstatus};

STDOUT:
#{child.out}

STDERR:
#{child.err}
EOF


      @logger.debug(debug_message)

      message = <<EOF
HandBrakeCLI execution failed with the following output.

STDOUT:
#{child.out}

STDERR:
#{child.err}
EOF
      raise TranscodeError.new(message) if child.status.exitstatus != 0
    end

    # Runs the specified command in it's own process using posix-spawn
    #
    # === Parameters
    # command(String):: system command to be executed
    #
    # === Return
    # result(POSIX::Spawn::Child):: The child process handled by posix-spawn
    def blocking_popen(command)
      POSIX::Spawn::Child.new(command)
    end

    # Uploads a file from disk to Google Storage
    #
    # === Parameters
    # dest_pathname(String):: The path and file name that will be stored in the Google Storage bucket.
    # source_file(String):: The full local file path of the file to upload to Google Storage
    #
    # === Raise
    # UploadError:: on any failure to upload to Google Storage
    def upload_file(dest_pathname, source_file)
      raise UploadError.new("The source file #{source_file} does not exist") unless File.exist? source_file
      begin
        @gstore.put_object(@gstore_bucket_name, dest_pathname, File.open(source_file, 'r'))
      rescue Excon::Errors::Error => e
        raise UploadError.new("Upload to Google Storage failed with: #{e.inspect}")
      end
    end

  end
end