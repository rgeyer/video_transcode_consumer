# encoding: utf-8

require 'rubygems'
require 'bundler'
require 'rspec/core/rake_task'
require 'rake/gempackagetask'
require 'rake/clean'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'rake'

desc 'Package gem'
gemtask = Rake::GemPackageTask.new(Gem::Specification.load('transcode_consumer.gemspec')) do |package|
  package.package_dir = 'pkg'
  package.need_zip = true
  package.need_tar = true
end

directory gemtask.package_dir

CLEAN.include(gemtask.package_dir)

# == Unit tests == #
spec_opts_file = "\"#{File.dirname(__FILE__)}/spec/spec.opts\""
RSPEC_OPTS = ['--options', spec_opts_file]

desc 'Run unit tests'
RSpec::Core::RakeTask.new do |t|
  t.rspec_opts = RSPEC_OPTS
end

namespace :spec do
  desc 'Run unit tests with RCov'
  RSpec::Core::RakeTask.new(:rcov) do |t|
    t.rspec_opts = RSPEC_OPTS
    t.rcov = true
    t.rcov_opts = %q[--exclude "spec"]
  end

  desc 'Print Specdoc for all unit tests'
  RSpec::Core::RakeTask.new(:doc) do |t|
    t.rspec_opts = ["--format", "documentation"]
  end
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

require 'rcov/rcovtask'
Rcov::RcovTask.new do |test|
  test.libs << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
  test.rcov_opts << '--exclude "gems/*"'
end

task :default => :test

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "transcode_consumer #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end