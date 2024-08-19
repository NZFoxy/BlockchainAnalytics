-- Create a table named Fraud_Wallets with three columns: address, identified_date, and notes
CREATE TABLE IF NOT EXISTS Fraud_Wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT UNIQUE NOT NULL,
    identified_date DATE NOT NULL,
    notes TEXT
);

-- Insert data into the Fraud_Wallets table
INSERT INTO Fraud_Wallets (address, identified_date, notes)
VALUES ('0x12345ABCDE', '2024-08-01', 'Identified by security team');

INSERT INTO Fraud_Wallets (address, identified_date, notes)
VALUES ('0xABCDE12345', '2024-08-05', 'Reported by multiple users');

INSERT INTO Fraud_Wallets (address, identified_date, notes)
VALUES ('0x67890FGHIJ', '2024-08-10', 'Suspicious transactions detected');
