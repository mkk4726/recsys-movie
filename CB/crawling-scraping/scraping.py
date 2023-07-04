import pandas as pd
from modules import get_review, save_result_to_pickle
import multiprocessing as mp
import tqdm

if __name__ == '__main__':
  
  title_basic_ratings_df = pd.read_csv('C:/Users/Hi/Desktop/CB(movie)/data/title_basic_ratings_df.csv', index_col=0)
  cond1 = title_basic_ratings_df['averageRating'] > 7.900000e+00 # 평균평점이 7점 이상 , 상위 75% 기준
  cond2 = title_basic_ratings_df['numVotes'] > 1.010000e+02 # 투표 수가 1000개 이상 , 상위 75% 기준
  cond3 = title_basic_ratings_df['startYear'] > 2.017000e+03 # 시작일이 2002년 이상 , 상위 50% 기준

  candidate_list = title_basic_ratings_df[cond1 & cond2 & cond3]['tconst'].tolist()
  

  n_CPU = mp.cpu_count()

  pool = mp.Pool(processes=n_CPU)
  
  results = []
  for result in tqdm.tqdm(pool.imap_unordered(
    get_review, candidate_list), total=len(candidate_list)):
    results.append(result)
  
  save_result_to_pickle('C:/Users/Hi/Desktop/CB(movie)/data\scraping_results/results_2.pickle', results)
  
  # with mp.Pool(n_CPU) as p:
  #   results = p.map(get_review, candidate_list[:8])
  
  

  

