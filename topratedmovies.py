from mrjob.job import MRJob
from mrjob.step import MRStep

class TopRatedMovies(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_aggregate_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        try:
            userID, movieID, rating, timestamp = line.strip().split('\t')
            yield movieID, (float(rating), 1)
        except ValueError:
            pass  # skip malformed lines

    def reducer_aggregate_ratings(self, movieID, values):
        total_rating = 0
        count = 0
        for rating, rating_count in values:
            total_rating += rating
            count += rating_count
        
        if count >= 100:  # only include movies rated at least 100 times
            average = total_rating / count
            yield movieID, round(average, 2)

if __name__ == '__main__':
    TopRatedMovies.run()
