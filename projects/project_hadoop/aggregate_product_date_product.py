from mrjob.job import MRJob, MRStep
import psycopg2

class AggregateProductbyDateProduct(MRJob):

    def mapper(self, _, line):
        item = line.strip().split(',')
        year = item[1][-4:]
        product_cat = item[2]
        yield (year, product_cat), int(item[4])

    def combiner(self, keys, values):
        yield keys, sum(values)

    def reducer_init(self):
        self.conn = psycopg2.connect(
            # host='host.docker.internal',
            host='localhost',
            user='postgres',
            password='password',
            database='postgres',
            port='5432'
        )

    def reducer(self, keys, values):
        year, product_cat = keys
        self.cur = self.conn.cursor()
        self.cur.execute("insert into total_order_year_product (year, product_category, total_order) values(%s, %s, %s)", (year, product_cat, sum(values)))
        # yield key, sum(values)

    # def store(self, key, values):

    def reducer_final(self):
        self.conn.commit()
        self.conn.close()
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    combiner=self.combiner,
                    reducer_init=self.reducer_init,
                    reducer=self.reducer,
                    reducer_final=self.reducer_final
                    )
        ]

if __name__ == '__main__':
    AggregateProductbyDateProduct.run()
    