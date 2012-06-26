require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe RGeyer::Worker do

  it 'does not raise an exception when queue names are specified' do
    worker = RGeyer::Worker.new "50.112.3.239", {:input_queue_name => "foobar"}
  end

  it "pops and downloads one job" do
    worker = RGeyer::Worker.new "50.112.3.239", 'gio_transcoding_demo', {:user => 'foo', :pass => 'barbaz'}
    puts worker.get_input_queue_status.to_yaml
    #worker.get_input_queue_status[:message_count].times do
      worker.do_job
    #end
  end
end
