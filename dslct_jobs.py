"""disco slct. Finds common patterns in logs."""

from disco.core import result_iterator
from disco.job import Job
from disco.worker.classic.func import sum_reduce, sum_combiner, \
                                      discodb_stream, chain_reader, input_stream
import string
from disco.util import kvgroup


class WordCounter(Job):
	"""Counts number of occurrences of words."""
	map_reader = staticmethod(chain_reader)

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


class WordToSentence(Job):
	"""Generates (Word, 1) => Sentence"""
	map_reader = staticmethod(chain_reader)

	@staticmethod
	def map(sentence, threshold):
		stripped_sentence = sentence.strip()
		for word in stripped_sentence.split():
			yield (word, 1), stripped_sentence


class SentenceWordJoiner(Job):
	"""Joins word counts and sentences.
	Input: [(string:Word, 0) => int:WordCount | (string:Word, 1) => [string:Sentence]]
	Output: [Sentence => [{Word: WordCount}]]
	"""
	map_reader = staticmethod(chain_reader)

	@staticmethod
        def reduce(iter, params):
		last_word = None
		for (word, weight), data in kvgroup(sorted(iter)):
			if weight==0:
				wordcount = list(data)
				wordcounts = {word: sum([int(count) for count in wordcount[0]])}
				last_word = word
			elif weight==1 and word==last_word:
				sentences = list(data)
				for sentence in sentences:
					yield sentence, wordcounts


	@staticmethod
	def partition(key, npartitions):
		word, _weight = key
		return hash(word) % npartitions


def combine(dict1, dict2):
	"""Combine two wordcount dicts.

	Example:
	>>> combine({}, {'a':5})
	{'a': 5}
	>>> combine({'a': 5}, {'b': 6})
	{'a': 5, 'b': 6}
	>>> combine({'b': 6}, {'a': 5})
	{'a': 5, 'b': 6}
	>>> combine({'a': 5}, {'a': 6})
	{'a': 11}
	>>> combine({}, {'a': 5})
	{'a': 5}
	"""
	from itertools import chain, groupby
	newmap_items = groupby(sorted(chain(dict1.iteritems(), dict2.iteritems())), lambda item: item[0])
	return dict([(word, sum([item[1] for item in items])) for word, items in newmap_items])


class ClusterConstructor(Job):
	"""Constructs the clusters.
	Input: [Sentence => [{Word: WordCount}]]
	Output: [[Word | None] => 1]
	"""
	map_reader = staticmethod(chain_reader)

	@staticmethod
	def reduce(iter, params):
		for sentence, wordcounts in kvgroup(sorted(iter)):
			unionized_wordcounts = reduce(wordcounts, combine, {})
			yield [word if unionized_wordcounts.has_key(word) else None for word in sentence.split()], 1


class Summer(Job):
	map_reader = staticmethod(chain_reader)
	reduce = staticmethod(sum_reduce)
	combiner = staticmethod(sum_combiner)

