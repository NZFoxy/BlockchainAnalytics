import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import pandas as pd

#Dataframe
def preprocess_data(df):
    df['value'] = df['value'].astype(float)
    df['gasPrice'] = df['gasPrice'].astype(float)
    df['gas'] = df['gas'].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').astype(int)

    df['fraudulent_label'] = np.where(df['value'] > 10000, 'red', 'green')

    X = df[['value', 'gasPrice', 'gas', 'timestamp']]
    y = df['fraudulent_label']

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, y, scaler

def train_model(X_train, y_train):
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    return clf

def evaluate_model(clf, X_test, y_test):
    y_pred = clf.predict(X_test)
    print(classification_report(y_test, y_pred))

def classify_transaction(transaction, scaler, clf):
    transaction_scaled = scaler.transform([transaction])
    prediction = clf.predict(transaction_scaled)
    return prediction[0]

def classify_all_transactions(df, scaler, clf):
    """Classify all transactions and count the number of each type."""
    X = df[['value', 'gasPrice', 'gas', 'timestamp']]
    X_scaled = scaler.transform(X)
    predictions = clf.predict(X_scaled)

    counts = pd.Series(predictions).value_counts()
    return counts.get('red', 0), counts.get('green', 0)
