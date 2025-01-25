from mrjob.job import MRJob

class MRAverageRating(MRJob):

    def mapper(self, _, line):
        if line.startswith("userId"):
            return
        userId, movieId, rating, timestamp = line.split(',')
        
        yield movieId, (float(rating), 1)

    def reducer(self, key, values):
        total_rating = 0
        total_count = 0
        for rating, count in values:
            total_rating += rating
            total_count += count
        yield key, total_rating / total_count

if __name__ == '__main__':
    MRAverageRating.run()
