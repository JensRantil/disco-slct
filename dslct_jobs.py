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
			yield (word, 0), str(sum(counts))
	
	combiner = staticmethod(sum_combiner)
	
	# Output format should be DiscoDB for fast retrieval
	reduce_output_stream = discodb_stream


class WordToSentence(Job):
	"""Generates (Word, 1) => Sentence"""
	map_reader = staticmethod(chain_reader)
	
	@staticmethod
	def map(sentence, threshold):
		for word in line.split():
			yield (word, 1), sentence


class ClusterConstructor(Job):
	"""Constructs the cluster elements.

	It is crucial the input to this reduce is sorted! In other words, set
	sort=True.
	"""

	@staticmethod
        def reduce(iter, params):
		from itertools import groupby
		
		for (word, weight), data in groupby(iter, lambda kv: kv[0]):
			if weight==0:
				wordcounts = dict([(word, count) for count in data])
				last_word = word
			elif weight==1:
				if word==last_word
				

	@staticmethod
	def partition(key, npartitions):
		word, _weight = key
		return hash(word) % npartitions

