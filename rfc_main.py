import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import fetch_data

from sklearn import datasets, metrics
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier

def main():
    print("Classifier script is running")

    # Load your dataset
    feature_columns = ['gasUsed', 'isError', 'value', 'confirmations', 'nonce', 'txreceipt_status']
    target_column = 'flag'
    transactions_dataset = fetch_data.create_dataset_from_df('Database/transactions.db', feature_columns, target_column)
    
    X = transactions_dataset.data
    y = transactions_dataset.target

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1, test_size=0.3, stratify=y)

    # Random Forest Classifier
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)

    # Predictions and accuracy
    y_pred = clf.predict(X_test)
    print("Random Forest Accuracy:", metrics.accuracy_score(y_test, y_pred))

    # Confusion matrix
    mat = metrics.confusion_matrix(y_test, y_pred)
    sns.heatmap(mat, annot=True, fmt='d', cbar=False)
    plt.xlabel('Predicted')
    plt.ylabel('True')
    plt.show()

    # Feature importances
    print("Feature Importances:", clf.feature_importances_)

    # Overfitting check using Random Forest with varying max_depth
    maxdepth_cv = []
    node_counts = []
    
    for k in range(2, 10):
        # Set the max_depth for the Random Forest
        clf.set_params(max_depth=k)
        clf.fit(X_train, y_train)
        
        # Perform cross-validation
        cv = cross_val_score(clf, X, y, cv=10)
        nodecount = clf.estimators_[0].tree_.node_count  # Get node count from the first tree
        
        print("max_depth={}".format(k), 
              "Average 10-Fold CV Score:{}".format(np.mean(cv)), 
              "Node count:{}".format(nodecount))
        
        maxdepth_cv.append(np.mean(cv))
        node_counts.append(nodecount)
    
    # Plotting the results
    plt.figure(figsize=(10, 5))
    plt.plot(range(2, 10), maxdepth_cv, label='CV Score')
    plt.plot(range(2, 10), node_counts, label='Node Count')
    plt.xlabel('Max Depth')
    plt.ylabel('Score / Node Count')
    plt.title('Overfitting Check')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()
