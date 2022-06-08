import SentimentClassification as sc
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import time

########################    REPRESENTAR EM BARRAS      ########################
########################    AS EMPRESAS, POR MULTIPLAS ########################
########################        MÉTRICAS               ########################

################## OBTER MEDIAS DE ESTATÍSTICAS POR EMPRESA + COMPETICAO ##############

def plot_sentiment_averages_combined(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### Get data
    
    dataset = sc.get_tweet_sentiment_combined(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block=time_block)
    
    ### Group by mean estatisticas relevantes
    
    dataset_grouped = dataset[['Time_Block','Bloco','Empresa', 'Tweet_Comments','Tweet_Likes', 'Tweet_Retweets',
                               'Tweet_Polarity', 'Tweet_Subjectivity', 'stdev_polarity','stdev_subjectivity']].groupby(
                                by = ['Time_Block','Bloco','Empresa'], as_index = False).mean()

    ### Plot Subjectivity + Plot Polarity
                                   
    dataset_grouped[['Tweet_Polarity', 'Tweet_Subjectivity']].set_index(dataset_grouped['Empresa']).plot.bar(subplots = True)
    
    ### Plot Comments, Likes, Retweets
    
    #dataset[['Tweet_Comments', 'Tweet_Likes','Tweet_Retweets']].set_index(dataset.index).plot.bar()
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
                                   
    return dataset_grouped

################## OBTER MEDIAS DE ESTATÍSTICAS POR EMPRESA ##############

def plot_sentiment_averages_company(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### Get data
    
    dataset = sc.get_tweet_sentiment_company(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block=time_block)
    
    time_start = time.time()
    
    ### Group by mean estatisticas relevantes
    
    dataset_grouped = dataset[['Time_Block','Bloco','Empresa', 'Tweet_Comments','Tweet_Likes', 'Tweet_Retweets',
                               'Tweet_Polarity', 'Tweet_Subjectivity', 'stdev_polarity','stdev_subjectivity']].groupby(
                                by = ['Time_Block','Bloco','Empresa'], as_index= False).mean()
     
    ### Plot Subjectivity + Plot Polarity
                                   
    dataset_grouped[['Tweet_Polarity', 'Tweet_Subjectivity']].set_index(dataset_grouped['Empresa']).plot.bar(subplots = True)
    
    ### Plot Comments, Likes, Retweets
    
    #dataset[['Tweet_Comments', 'Tweet_Likes','Tweet_Retweets']].set_index(dataset.index).plot.bar()     

    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)                          
                                   
    return dataset_grouped

################## OBTER MEDIAS DE ESTATÍSTICAS POR COMPETICAO ##############

def plot_sentiment_averages_competitor(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### Get data
    
    dataset = sc.get_tweet_sentiment_competitor(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)
    
    ### Group by mean estatisticas relevantes
    
    dataset_grouped = dataset[['Time_Block','Bloco','Empresa', 'Tweet_Comments','Tweet_Likes', 'Tweet_Retweets',
                               'Tweet_Polarity', 'Tweet_Subjectivity', 'stdev_polarity','stdev_subjectivity']].groupby(
                                by = ['Time_Block','Bloco','Empresa'], as_index= False).mean()
     
    ### Plot Subjectivity + Plot Polarity
                                   
    dataset_grouped[['Tweet_Polarity', 'Tweet_Subjectivity']].set_index(dataset_grouped['Empresa']).plot.bar(subplots = True)
    
    ### Plot Comments, Likes, Retweets
    
    #dataset[['Tweet_Comments', 'Tweet_Likes','Tweet_Retweets']].set_index(dataset.index).plot.bar()   

    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)                            
                                   
    return dataset_grouped
  
########################    RANKEAR AS EMPREAS         ########################
########################    COM BASE EM CADA UMA DAS   ########################
########################        MÉTRICAS               ########################   

################## RANKING EMPRESAS ###############################3

def ranking_empresas_combined(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### Get data
    
    dataset = sc.get_tweet_sentiment_combined(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)
    
    ### Group by mean estatisticas relevantes
    
    dataset_grouped = dataset[['Time_Block','Bloco','Empresa', 'Tweet_Comments','Tweet_Likes', 'Tweet_Retweets',
                               'Tweet_Polarity', 'Tweet_Subjectivity', 'stdev_polarity','stdev_subjectivity']].groupby(
                                by = ['Time_Block','Bloco','Empresa'], as_index = False).mean()
    
    ### Rank por maior rank médio: as 5 métricas usad,as pesam todas o mesmo
    ###     Dense rank faz ranking como o seguinte:  1 - 2 - 3 - 3 - 4
    ###     Rank normal faz ranking como o seguinte: 1 - 2 - 3.5 - 3.5 - 5
                                   
    dataset_ranked =  pd.DataFrame(data ={  'Time_Block' : dataset_grouped['Time_Block'],
                                            'Bloco' : dataset_grouped['Bloco'],
                                            'Empresa': dataset_grouped['Empresa'],                                            
                                            'Tweet_Comments':dataset_grouped['Tweet_Comments'].rank(method = 'dense'),
                                            'Tweet_Likes':dataset_grouped['Tweet_Likes'].rank(method = 'dense'),
                                            'Tweet_Retweets':dataset_grouped['Tweet_Retweets'].rank(method = 'dense'),
                                            'Tweet_Polarity':dataset_grouped['Tweet_Polarity'].rank(method = 'dense'),
                                            'Tweet_Subjectivity':dataset_grouped['Tweet_Subjectivity'].rank(method = 'dense'),
                                            'stdev_polarity_NotInRank':dataset_grouped['stdev_polarity'].rank(method = 'dense'),
                                            'stdev_subjectivity_NotInRank':dataset_grouped['stdev_subjectivity'].rank(method = 'dense')
                                          })
    
    dataset_ranked['Company_Rank'] = (dataset_ranked['Tweet_Comments'] + dataset_ranked['Tweet_Likes'] +
                                     dataset_ranked['Tweet_Retweets'] + dataset_ranked['Tweet_Polarity'] +
                                     dataset_ranked['Tweet_Subjectivity']).rank(method = 'dense')
    
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    ### Return apenas company_rank
        
    return dataset_ranked

if __name__ == '__main__' :
    
    
    
    a=ranking_empresas_combined('Netflix', count= 20)
    #print(1)
    #dataset[['Tweet_Polarity', 'Tweet_Subjectivity']].set_index(dataset.index).plot.bar()
    #dataset[['Tweet_Comments', 'Tweet_Likes','Tweet_Retweets']].set_index(dataset.index).plot.bar()