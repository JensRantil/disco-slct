"""disco slct. Finds common patterns in logs.

author -- Jens Rantil <jens.rantil@gmail.com>
"""
import sys
from optparse import OptionParser

from disco.core import result_iterator

from dslct_jobs import WordCounter, WordPruner, ClusterConstructor


def format_common_line(arr):
	"""Formats a common line from array form to string form.

	Example:
	>>> format_common_line(["hej", "ba", None]):
	'hej ba *'
	"""
	return string.join(map(lambda word: word if word else "*"), " ")


def run(options, inputurl):
	"""The actual slct application."""
	# The number '20' is taken out of thin air just to spread the load a
	# little.
	counter = WordCounter().run(input=[inputurl], partitions=20)
	counter.wait()
	pruner = WordPruner().run(input=counter.wait(), params=options.support)
	pruner.wait()
	counter.purge()
	constructor = ClusterConstructor().run(input=pruner.wait(),
						params=pruner.wait())
	constructor.wait()
	# TODO: In the future have the option to keep words from job 2 and reuse them
	pruner.purge()
	for commonline, count in result_iterator(constructor.wait()):
		print count, format_common_line(commonline)


def main(argv):
	"""Parses command line arguments and calls run(...)."""
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

