
Gem::Specification.new do |gem|
  gem.name = "transcode_consumer"
  gem.version = "0.0.1"
  gem.homepage = "http://github.com/rgeyer/transcode_consumer"
  gem.license = "MIT"
  gem.summary = %Q{Processes video transcoding jobs that transcode_producer added to an AMQP job list}
  gem.description = gem.summary
  gem.email = "me@ryangeyer.com"
  gem.authors = ["Ryan J. Geyer"]
  gem.executables << 'transcode_consumer'

  gem.add_dependency('bunny', '~> 0.7')
  gem.add_dependency('fog', '~> 1.3')
  gem.add_dependency('trollop', '~> 1.16')
  gem.add_dependency('json', '~> 1.7')
  gem.add_dependency('posix-spawn', '~> 0.3.6')

  gem.files = Dir.glob("{lib,bin}/**/*") + ["LICENSE.txt", "README.rdoc"]
end