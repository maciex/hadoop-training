from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapMoviesToRatings, reducer=self.countRatings)
		]
	
	def mapMoviesToRatings(self, _, line):
		(userId, movieId, rating, timestamp) = line.split('\t')
		yield movieId, 1

	def countRatings(self, movieId, values):
		yield movieId, sum(values)

if __name__ == '__main__':
	RatingsBreakdown.run()
