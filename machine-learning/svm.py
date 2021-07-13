import pandas as pd
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler

if __name__ == "__main__":
    scaler = StandardScaler()
    df_properties = pd.read_csv('../feature_generation/cleaned.csv')
    df_properties = df_properties[(df_properties['propertyType'] == 'Condominium') | (df_properties['propertyType'] == 'Apartment') | (df_properties['propertyType'] == 'Executive Condominium')]
    feature_columns = ['rentalYield', 'leaseRemaining', 'noOfTransactions', 'distanceToCentral', 'distanceToNearestMall', 'distanceToNearestMrt']
    predict_columns = ['rentalYieldNow', 'leaseRemainingNow', 'noOfTransactions', 'distanceToCentral', 'distanceToNearestMall', 'distanceToNearestMrt']
    X = df_properties[feature_columns].to_numpy()
    X = scaler.fit_transform(X)
    Y = df_properties['annualAppreciation'].to_numpy()

    svr = SVR(kernel='linear')
    results = svr.fit(X, Y)
    df_results = pd.DataFrame({'Coeffcients': results.coef_.tolist()[0], 'Features': feature_columns})
    df_results.sort_values('Coeffcients', ascending=False, inplace=True)
    print(df_results)
    X = df_properties[predict_columns].to_numpy()
    X = scaler.fit_transform(X)
    predictions = svr.predict(X)
    df_properties['predicted_annual_appreciation'] = predictions.tolist()
    df_properties.to_csv('predictions.csv')
