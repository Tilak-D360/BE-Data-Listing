import pandas as pd
def work():
    suffixes = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    df = pd.DataFrame(suffixes, columns = ['suffix'])
    print(df)
    df.to_csv('gs://be-data-listing-bucket/suffix-data.csv', index = False)
    df=  pd.read_csv('gs://be-data-listing-bucket/suffix-data.csv')
    for suffix in df['suffix']:
        print(suffix)
    # df.to_csv('data.csv')

if __name__ == "__main__":
    work()