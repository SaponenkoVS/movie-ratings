from mrjob.job import MRJob


class MRAverageRating(MRJob):

    def configure_args(self):
        super(MRAverageRating, self).configure_args()
        # Dodajemy argument dla ścieżek do plików movies.csv i ratings.csv
        self.add_file_arg('--movies', help='Ścieżka do pliku movies.csv')

    def mapper(self, _, line):
        # Sprawdzamy, czy to linia z pliku ratings.csv (userId, movieId, rating, timestamp)
        if line.startswith("userId"):  # Pomijamy nagłówek
            return

        # Przetwarzanie linii z pliku ratings.csv
        parts = line.split(",")
        if len(parts) == 4:  # Sprawdzamy, czy linia jest poprawna
            userId, movieId, rating, timestamp = parts
            # Emitujemy movieId i ocenę
            yield movieId, ("rating", float(rating), 1)

    def combiner(self, movieId, values):
        # Akumulujemy oceny i ich liczbę w combinerze
        total_rating = 0
        count = 0
        for value in values:
            if value[0] == "rating":
                total_rating += value[1]
                count += value[2]
        # Zwracamy pośredni wynik do reduktora
        yield movieId, ("rating", total_rating, count)

    def reducer(self, movieId, values):
        # Przechowujemy tytuł filmu
        title = None
        total_rating = 0
        count = 0

        # Iterujemy przez wszystkie wartości przypisane do movieId
        for value in values:
            if value[0] == "rating":
                total_rating += value[1]
                count += value[2]
        
        # Obliczamy średnią ocenę
        average_rating = total_rating / count if count > 0 else 0

        # Odczytujemy tytuł filmu z pliku movies.csv
        with open('movies.csv', 'r', encoding='utf-8') as movies_file:
            for line in movies_file:
                if line.startswith(movieId):  # Znajdujemy tytuł filmu
                    parts = line.split(",")
                    if len(parts) > 1:
                        title = parts[1].strip()
                        break
        
        # Zwracamy tytuł filmu oraz średnią ocenę
        yield title, average_rating

if __name__ == '__main__':
    MRAverageRating.run()
