"""disco slct. Finds common patterns in logs."""
import sys
from optparse import OptionParser

from disco.job import Job
from disco.worker.classic.func import sum_reduce, sum_combiner, discodb_stream, nop_map


class WordCounter(Job):
	"""Counts number of occurrences of words."""

	@staticmethod
	def map(line, _params):
		for word in line.split():
			# TODO: Make is possible to specify what is considered 'whitespace'
			yield word, 1

	reduce = sum_reduce
	combiner = sum_combiner


class WordPruner(Job):
	"""Prunes words that are not frequent enough."""

	@staticmethod
	def map((word, count), threshold):
		if count >= threshold:
			yield word, count

	reduce = sum_reduce
	combiner = sum_combiner
	
	# Output format should be DiscoDB for fast retrieval
	reduce_output_stream = discodb_stream


class ClusterConstructor(Job):
	"""Constructs the cluster elements."""

	@staticmethod
	def map(line, params):
		import string
		# TODO: Check if this is a candidate using DiscoDB. In that case, yield
		master_host = params['master_host']
		wordcounturl = params['wordcounturl']
		words = line.split()
		querystring = string.join(words, " | ") # TODO: Needs escaping
		job = Query().run(input=wordcounturl, params=querystring)
		cluster = {}
		for word, count in result_iterator(job.wait()):
			cluster[word] = count
		if cluster:
			yield map(lambda word: cluster.get(word, None), words), 1
		
        reduce = sum_reduce
        combiner = sum_combiner


class Query(Job):
	"""Job that queries a bunch of DiscoDB instances."""
	# Necessary?
	#map_input_stream = (input_stream,)
	map = staticmethod(nop_map)

	@staticmethod
	def map_reader(discodb, size, url, params):
		for k, vs in discodb.metaquery(params):
			yield k, list(vs)


def format_common_line(arr):
	"""Formats a common line from array form to string form.

	Example:
	>>> format_common_line(["hej", "ba", None]):
	'hej ba *'
	"""
	return string.join(map(lambda word: word if word else "*"), " ")


def run(options, inputurl):
	"""The actual slct application."""
	counter = WordCounter().run(input=inputurl)
	counter.wait()
	return 0
	pruner = WordPruner().run(input=counter.wait(), params=options.support)
	pruner.wait()
	counter.purge()
	contructor = ClusterConstructor().run(input=pruner.wait(),
						params={'master_host': options.master,
							'wordcounturl': prunter.wait()})
	constructor.wait()
	# TODO: In the future have the option to keep words from job 2 and reuse them
	pruner.purge()
	for commonline, count in result_iterator(constructor.wait()):
		print count, format_common_line(commonline)


def main(argv):
	"""Parses command line options and calls run(...)."""
	parser = OptionParser(usage="%prog [options] inputurl")
	parser.add_option("-s", "--support", type="int",
                          help="the least support count for each fingerprint")
	(options, args) = parser.parse_args(argv)
	if len(args) != 2:
		print "Wrong number of arguments."
		print
		parser.print_help()
		return 1
	logfileurl, = args[1:]

	if not options.support:
		print "You must specify a support. Exitting..."
		return 1
	return run(options, logfileurl)
	

if __name__=="__main__":
	sys.exit(main(sys.argv))

