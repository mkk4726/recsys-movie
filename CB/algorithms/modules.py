import requests as re
from bs4 import BeautifulSoup as bs
import pandas as pd
import pickle

def load_cosine_df():
  return load_pickle('C:/Users/Hi/Desktop/CB(movie)/data/similarity/cosine_sim_df_1.pickle')

def get_CB_recomm(df1:"pd.DataFrame", df2:"pd.DataFrame", title_id:str):
  """CB 기반 추천 알고리즘

  Args:
      df1 (pd.DataFrame): title_basic_df
      df2 (pd.DataFrame): cosine_df
      title_id (str): title id
  """
  print(f"about : {find_title(df1, title_id)}")

  candidate_series = df2[title_id].sort_values(ascending=False)[1:10]
  
  for title_id, sim in zip(candidate_series.index, candidate_series):
    title = find_title(df1, title_id)
    url = f'https://www.imdb.com/title/{title_id}/?ref_=fn_al_tt_1'
    print(f"title: {title} / sim: {sim:.4f} / url: {url}")

def load_title_basic_df() -> "pd.DataFrame":
  return load_pickle('C:/Users/Hi/Desktop/CB(movie)/data/IMDb_Non-Commercial_Datasets/title_basic.pickle')

def find_title(df:"pd.DataFrame", title_id:str) -> str:
  """convert title id to name

  Args:
      df (pd.DataFrame): title_basic_df
      title_id (str): title_id

  Returns:
      str: title
  """
  return df.loc[title_id, 'primaryTitle']


# 결과 저장하기.
def save_result_to_pickle(path, result):
  with open(path, 'wb') as handle:
    pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
# 결과 열기
def load_pickle(path):
  with open(path, 'rb') as handle:
    return pickle.load(handle)

def get_review(title_id: str, verbose: bool=False) -> tuple[str, "pd.DataFrame"]:
  
  """IMDb에서 review data를 sraping해오는 func

  Args:
      title_id (str): 영화별 고유 id
      verbose (bool, optional): scraping 과정을 출력할건지 여부

  Returns:
      tuple[str, DataFrame]: title_id, [rating, title, review,	user, date] column을 가진 dataframe
  """
  
  # url = f'https://www.imdb.com/title/{title_id}/reviews?sort=totalVotes&dir=desc&ratingFilter=0'
  # spolier 같은 경우는 접혀있어서 눌러서 봐야함. 따라서 오류가 생긴다. Hide Spoiler옵션 누른 url로 대체
  url = f'https://www.imdb.com/title/{title_id}/reviews?spoiler=hide&sort=totalVotes&dir=desc&ratingFilter=0'

  res = re.get(url)
  if res.status_code == 200:
    html = res.text
    soup = bs(html, 'html.parser')
  else:
    print(res.status_code)
    return 
  
  text_list = []
  rating_list = []
  title_list = []
  user_list = []
  date_list = []
  review_df = pd.DataFrame()
  
  try:
    i = 1
    while True:
      main_selector = f'#main > section > div.lister > div.lister-list > div:nth-child({i}) > div.review-container > div.lister-item-content'
      
      text_selector = f'{main_selector} > div.content > div.text.show-more__control'
      text = soup.select(text_selector)[0].text
      
      rating_selector = f'{main_selector} > div.ipl-ratings-bar > span > span:nth-child(2)'
      rating = soup.select(rating_selector)[0].text
            
      title_selector = f'{main_selector} > a'
      title = soup.select(title_selector)[0].text
      
      user_selector = f'{main_selector} > div.display-name-date > span.display-name-link > a'
      user = soup.select(user_selector)[0].text
      
      date_selector = f'{main_selector} > div.display-name-date > span.review-date'
      date = soup.select(date_selector)[0].text
      
      text_list.append(text)
      rating_list.append(rating)
      title_list.append(title)
      user_list.append(user)
      date_list.append(date)
        
      if verbose:
        print(f"{i} is done")
        
      i += 1
  except:
    a = 1 # 아무 의미 없음
    
  if verbose:
    print('{title_id} end')
  
  review_df['review'] = text_list
  review_df['rating'] = rating_list
  review_df['title'] = title_list
  review_df['user'] = user_list
  review_df['date'] = date_list
  
  return title_id, review_df

if __name__ == '__main__':
  print(load_pickle('C:/Users/Hi/Desktop/CB(movie)/data\scraping_results/results_1.pickle'))