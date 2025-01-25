from mrjob.job import MRJob
import codecs


class MRAverageRating(MRJob):

    def configure_args(self):
        super(MRAverageRating, self).configure_args()
        # Add argument for the movies file
        self.add_file_arg('--movies', help='Path to the movies.csv file')

    def mapper(self, _, line):
        # Skip the header line in the ratings file
        if line.startswith("userId"):
            return

        # Process a line from the ratings.csv file
        parts = line.split(",")
        if len(parts) == 4:  # Ensure the line is correct
            userId, movieId, rating, timestamp = parts
            # Emit movieId and rating information
            yield movieId, ("rating", float(rating), 1)

    def combiner(self, movieId, values):
        # Accumulate the ratings and counts in the combiner
        total_rating = 0
        count = 0
        for value in values:
            if value[0] == "rating":
                total_rating += value[1]
                count += value[2]
        # Return intermediate results to the reducer
        yield movieId, ("rating", total_rating, count)

    def reducer(self, movieId, values):
        # Store movie title (initialized to None)
        title = None
        total_rating = 0
        count = 0

        # Iterate through all values associated with the movieId
        for value in values:
            if value[0] == "rating":
                total_rating += value[1]
                count += value[2]
       
        # Compute the average rating
        average_rating = total_rating / count if count > 0 else 0

        # Read the movie titles from the distributed movies.csv file
        if self.options.movies:
            title = self.get_movie_title(movieId)

        # Yield the movie title and average rating
        yield title, average_rating

    def get_movie_title(self, movieId):
        """Reads the movies.csv file and fetches the title corresponding to the movieId."""
        title = None

        # This file has been distributed to the worker nodes
        with codecs.open(self.options.movies, 'r', 'utf-8') as movies_file:
            for line in movies_file:
                if line.startswith(movieId):  # Find the movie title
                    parts = line.split(",")
                    if len(parts) > 1:
                        title = parts[1].strip()
                        break
        return title


if __name__ == '__main__':
    MRAverageRating.run()
