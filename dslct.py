"""disco slct. Finds common patterns in logs.

author -- Jens Rantil <jens.rantil@gmail.com>
"""
import sys
from optparse import OptionParser
import string

from disco.core import result_iterator

from dslct_jobs import WordToSentence, WordCounter, WordPruner, \
                       SentenceWordJoiner, ClusterConstructor, Summer


def format_common_line(arr):
	"""Formats a common line from array form to string form.

	Example:
	>>> format_common_line(["hej", "ba", None]):
	'hej ba *'
	"""
	words = map(lambda word: word if word else "*", arr)
	return string.join(words, " ")


def print_result(url, label=None):
	"""Prints the result of a job to stdout.

	Used for debugging.
	"""
	if label:
		print "=" * 40
		print label
		print "=" * 40
	for key, val in result_iterator(url):
		print "%s: \t%s" % (key, val)


def do_purging(job, options):
	if options.purge:
		job.purge()


def run(options, inputurl):
	"""The actual slct application."""
	# The number '20' is taken out of thin air just to spread the load a
	# little.
	N_REDUCE_PARTITIONS = 20

	sentenceworder = WordToSentence().run(input=[inputurl])
	counter = WordCounter().run(input=[inputurl], partitions=N_REDUCE_PARTITIONS)
	if options.debug:
		print_result(counter.wait(), "Counter")
	pruner = WordPruner().run(input=counter.wait(), params=options.support)
	pruner.wait()
	if options.debug:
		print_result(pruner.wait(), "Pruner")
	do_purging(counter, options)
	if options.debug:
		print_result(sentenceworder.wait(), "SentenceWorder")
	joiner = SentenceWordJoiner().run(input=sentenceworder.wait()+pruner.wait(),
						partitions=N_REDUCE_PARTITIONS)
	joiner.wait()
	if options.debug:
		print_result(joiner.wait(), "Joiner")
	do_purging(sentenceworder, options)
	do_purging(pruner, options)
	cconstructor = ClusterConstructor().run(input=joiner.wait(), partitions=N_REDUCE_PARTITIONS)
	cconstructor.wait()
	if options.debug:
		print_result(cconstructor.wait(), "Cconstructor")
	do_purging(joiner, options)
	summer = Summer().run(input=cconstructor.wait(), partitions=N_REDUCE_PARTITIONS)

	# TODO: In the future have the option to keep words from job 2 and reuse them
	for commonline, count in result_iterator(cconstructor.wait()):
		print count, format_common_line(commonline)

	do_purging(cconstructor, options)
	do_purging(summer, options)


def main(argv):
	"""Parses command line arguments and calls run(...)."""
	parser = OptionParser(usage="%prog [options] inputurl")
	parser.add_option("-s", "--support", type="int",
                          help="the least support count for each fingerprint")
	parser.add_option("-d", "--debug", action="store_true",
                          default=False, help="Enable debug output. Only use"
                                                " this for small input!")
	parser.add_option("-N", "--no-purging", dest="purge",
                          action="store_false", default="true",
                          help="Disable automatic purging of used"
                                               " job results.")
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

