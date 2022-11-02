from mrjob.job import MRJob, MRStep
import psycopg2

class AggregateProduct(MRJob):

    def mapper(self, _, line):
        item = line.strip().split(',')
        year = item[1][-4:]
        yield year, int(item[4])

    def reducer_init(self):
        self.conn = psycopg2.connect(
            # host='host.docker.internal',
            host='localhost',
            user='postgres',
            password='password',
            database='postgres',
            port='5432'
        )

    def reducer(self, key, values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into total_order_yearly (year, total_order) values(%s, %s)", (key, sum(values)))
        # yield key, sum(values)

    # def store(self, key, values):

    def reducer_final(self):
        self.conn.commit()
        self.conn.close()
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    reducer_init=self.reducer_init,
                    reducer=self.reducer,
                    reducer_final=self.reducer_final
                    )
        ]

if __name__ == '__main__':
    AggregateProduct.run()
    