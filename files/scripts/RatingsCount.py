from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsCount(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapMoviesToRatings, reducer=self.countRatings)
		]
	
	def mapMoviesToRatings(self, _, line):
		(userId, movieId, rating, timestamp) = line.split('\t')
		yield rating, 1

	def countRatings(self, rating, values):
		yield rating, sum(values)

if __name__ == '__main__':
	RatingsCount.run()
