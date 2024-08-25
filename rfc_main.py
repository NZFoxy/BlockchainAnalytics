import os
import pandas as pd
import numpy as np
#%pip install seaborn ///// run this if you dont have seaborn yet
import seaborn as sns
import matplotlib.pyplot as plt
#%matplotlib inline
import fetch_data

from sklearn import datasets, metrics
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier




def rfc_main():
    
     # Fetch API key from environment variable
    api_key = os.getenv('POLYGONSCAN_API_KEY')
    if not api_key:
        raise ValueError("API Key is not set in environment variables")
    print("Classifier script is running")

    iris = datasets.load_iris()
    
    #criterias for which the transaction is classified on
    feature_columns = ['gasUsed', 'isError', 'value', 'confirmations', 'nonce', 'txreceipt_status']

    #the "label"
    target_column = 'flag'

    transactions_dataset = fetch_data.create_dataset_from_df('Database/transactions.db',feature_columns,target_column)

    #print(transactions_dataset.DESCR)

    print(transactions_dataset.data)

    print(transactions_dataset.target)

    print(transactions_dataset.feature_names)


    #print(transactions_dataset.target_names)

    X = transactions_dataset.data
    y = transactions_dataset.target

    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 1, test_size = 0.3, stratify = y)

    clf = RandomForestClassifier(n_estimators = 100, random_state = 42)

    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    print(metrics.accuracy_score(y_test, y_pred))

    mat = metrics.confusion_matrix(y_test, y_pred)

    print(mat)

    sns.heatmap(mat, annot = True, fmt = 'd', cbar = False, xticklabels = transactions_dataset.target_names, yticklabels = transactions_dataset.target_names)
    plt.xlabel('Predicted')
    plt.ylabel('True')

    print(clf.feature_importances_)


    '''
    iris.target_names

    # print(iris.DESCR) shows iris dataset description

    X = iris.data
    y = iris.target

    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 1, test_size = 0.3, stratify = y)

    clf = RandomForestClassifier(n_estimators = 100, random_state = 42)

    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    print(metrics.accuracy_score(y_test, y_pred))

    mat = metrics.confusion_matrix(y_test, y_pred)

    print(mat)

    sns.heatmap(mat, annot = True, fmt = 'd', cbar = False, xticklabels = iris.target_names, yticklabels = iris.target_names)
    plt.xlabel('Predicted')
    plt.ylabel('True')


    #help(clf)

    clf.feature_importances_

    #print(iris) shows iris dataset
    '''

if __name__ == "__rfc_main__":
    rfc_main()