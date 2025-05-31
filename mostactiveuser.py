from mrjob.job import MRJob
from mrjob.step import MRStep

class MostActiveUsers(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_users,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_users(self, _, line):
        try:
            userID, movieID, rating, timestamp = line.strip().split('\t')
            yield userID, 1
        except ValueError:
            pass  # skip malformed lines

    def reducer_count_ratings(self, userID, counts):
        total = sum(counts)
        yield userID, total

if __name__ == '__main__':
    MostActiveUsers.run()
