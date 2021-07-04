import pandas as pd
import numpy as np

from transformer import Pipeline
import config
import warnings
warnings.filterwarnings("ignore")


pipeline = Pipeline(lastdayfrom =  config.LAST_DAY_FROM)




if __name__ == '__main__':
    
    # load data set
    print('Loading data file started')
    visitor_log_data = pd.read_csv(config.PATH_TO_VISITOR_LOG_DATA)
    user_data = pd.read_csv(config.PATH_TO_USER_DATA)
    input_feature = pd.read_csv(config.PATH_TO_INPUT_FEATURE)
    print('Loading data file complete')
    pipeline.fit(visitor_log_data=visitor_log_data,user_data=user_data,input_feature=input_feature)
    