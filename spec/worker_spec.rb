require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe RGeyer::TranscodeConsumer do

  it 'does not raise an exception when queue names are specified' do
    transcode_consumer = RGeyer::TranscodeConsumer.new "50.112.3.239", {:input_queue_name => "foobar"}
  end

  it "pops and downloads one job" do
    transcode_consumer = RGeyer::TranscodeConsumer.new "50.112.3.239", 'gio_transcoding_demo', {:user => 'foo', :pass => 'barbaz'}
    puts transcode_consumer.get_input_queue_status.to_yaml
    #transcode_consumer.get_input_queue_status[:message_count].times do
      transcode_consumer.do_job
    #end
  end
end
