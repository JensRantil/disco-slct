"""disco slct. Finds common patterns in logs."""

from disco.core import result_iterator
from disco.job import Job
from disco.worker.classic.func import sum_reduce, sum_combiner, \
                                      discodb_stream, nop_map, chain_reader, input_stream
import string
from disco.util import kvgroup


class WordCounter(Job):
	"""Counts number of occurrences of words."""

	@staticmethod
	def map(line, _params):
		for word in line.split():
			# TODO: Make is possible to specify what is considered 'whitespace'
			yield word, 1

	reduce = staticmethod(sum_reduce)
	combiner = staticmethod(sum_combiner)


class WordPruner(Job):
	"""Prunes words that are not frequent enough.

	Output is written to DiscoDB for fast indexing.
	"""
	map_reader = staticmethod(chain_reader)

	@staticmethod
	def map((word, count), threshold):
		if count >= threshold:
			yield word, count

	@staticmethod
	def reduce(iter, params):
		for word, counts in kvgroup(sorted(iter)):
			yield word, str(sum(counts))
	
	combiner = staticmethod(sum_combiner)
	
	# Output format should be DiscoDB for fast retrieval
	reduce_output_stream = discodb_stream


class ClusterConstructor(Job):
	"""Constructs the cluster elements."""

	@staticmethod
	def map(line, wordcounturl):
		# TODO: Check if this is a candidate using DiscoDB. In that case, yield
		words = line.split()
		querystring = string.join(words, " | ") # TODO: Needs escaping
		job = Query().run(input=wordcounturl, params=querystring)
		cluster = {}
		for word, count in result_iterator(job.wait()):
			cluster[word] = count
		if cluster:
			yield map(lambda word: cluster.get(word, None), words), 1
		
        reduce = staticmethod(sum_reduce)
        combiner = staticmethod(sum_combiner)


class Query(Job):
	"""Job that queries a bunch of DiscoDB instances."""
	# Necessary?
	map_input_stream = (input_stream,)
	map = staticmethod(nop_map)

	@staticmethod
	def map_reader(discodb, size, url, params):
		for k, vs in discodb.metaquery(params):
			yield k, list(vs)

