from data.data_common.dependencies.dependencies import artifacts_repository, artifact_scores_repository, persons_repository
from data.data_common.services.artifacts_service import ArtifactsService
from ai.train.profile_predictions_data import ProfilePredictionsData
import pandas as pd
import numpy as np
import joblib
from sklearn.linear_model import Ridge 
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

from common.genie_logger import GenieLogger

logger = GenieLogger()
params = ['Time Focus (past vs present)','Self-Learner','Logic/Analysis vs Feeling/Intuition','Technical','Numbers','Perfectionist','Intuitive','Feeling','Risk Aversion vs Novelty','Tradition','Security','Ideation','Social Influence','Relator','Skeptic','Self-Branding/Status Consideration','Competition','Command','Achiever','Responsive vs Disruptive','Pace','Associative vs Structured','Values ','Altruistic','Stress Tolerance','Turbulent','Dramatic','Hedonism']
all_profiles = [
    "Innovator",
    "Go-Getter",
    "Analytical",
    "Social",
    "Emotional",
    "Thorough",
]

class ProfileParamWeights:
    def __init__(self):
        self.persons_repository = persons_repository()
        self.artifacts_repository = artifacts_repository()
        self.artifact_scores_repository = artifact_scores_repository()
        self.artifacts_service = ArtifactsService()
        self.profile_predictions_data = ProfilePredictionsData()
        self.predictions = self.profile_predictions_data.predictions
        self.people_anaylsed = []

        self.data = {}
        for param in params:
            self.data[param] = []

    def prepare_data_for_training(self):
        unique_profile_uuids = self.artifacts_service.get_unique_profiles()
        profiles_param_scores = {}
        people = []
        for profile_uuid in unique_profile_uuids:
            person = self.persons_repository.get_person(profile_uuid)
            if not person:
                continue
            profile_param_score = self.artifacts_service.calculate_overall_params(person.name, profile_uuid)
            if not profile_param_score:
                continue
            if self.predictions.get(person.name) is None:
                logger.info(f"No predictions found for person {person.name}")
                continue
            people.append({'name' : person.name, 'traits' : profile_param_score, 'profiles' : self.predictions[person.name]})
            profiles_param_scores[profile_uuid] = profile_param_score
            self.people_anaylsed.append(person.name)
        
        # for profile_uuid, profile_param_score in profiles_param_scores.items():
        #     logger.info(f"Profile param score for {profile_uuid[:5]}: {profile_param_score}")

        if people:
            training_data = self.create_data_dictionary(people)
            i = 0

        for prediction in self.predictions:
            if prediction not in self.people_anaylsed:
                logger.info(f"Person {prediction} not found in the data")

        self.train2(training_data)

    def create_data_dictionary(self, people):
        """
        Creates a dictionary of lists, where each key is either:
        - a trait from params
        - a profile from profiles_list
        and the value is a list of length == len(people),
        containing that trait/profile's value for each person (or None if missing).
        """
        data = {}

        # Initialize each trait in data with an empty list
        for trait in params:
            data[trait] = []

        # Initialize each profile in data with an empty list
        for profile in all_profiles:
            data[profile] = []

        # Fill in values from people
        for person in people:
            # 1) For the traits
            for trait in params:
                if trait in person["traits"]:
                    data[trait].append(person["traits"][trait])
                else:
                    # If the person doesn't have that trait score, append None
                    data[trait].append(None)

            # 2) For the profiles
            for profile in all_profiles:
                if profile in person["profiles"]:
                    data[profile].append(person["profiles"][profile])
                else:
                    data[profile].append(None)
        
        # Fill in missing values with a default value (150)
        for key, values in data.items():
            if all(v is None for v in values):
                values = [150 for v in values]
                data[key] = values

        return data

    def train2(self, data):
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Define feature and label columns
        feature_cols = df.columns[:28]
        label_cols = df.columns[28:]

        # Handle missing values: Fill NaN with column mean
        df[feature_cols] = df[feature_cols].apply(lambda x: x.fillna(x.mean()), axis=0)

        # Save the mean values for future predictions
        means = df[feature_cols].mean().to_dict()
        joblib.dump(means, "trait_means.pkl")

        # Normalize trait scores (100-200 → 0-1)
        scaler = MinMaxScaler()
        df[feature_cols] = scaler.fit_transform(df[feature_cols])

        # Save the scaler for future use
        joblib.dump(scaler, "scaler.pkl")

        # Train Ridge Regression Models for Each Profile
        models = {}
        for class_label in label_cols:
            model = Ridge()
            model.fit(df[feature_cols], df[class_label])
            joblib.dump(model, f"{class_label}_ridge_regression.pkl")
            models[class_label] = model

        print("\nModels, scaler, and mean trait values saved successfully!")

    def train(self, data):
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Define feature and label columns
        feature_cols = df.columns[:28]
        label_cols = df.columns[28:]

        # 1. Handle missing values: Fill NaN with column mean
        df[feature_cols] = df[feature_cols].apply(lambda x: x.fillna(x.mean()), axis=0)

        # Check if NaNs remain
        if df[feature_cols].isnull().sum().sum() > 0:
            print("Warning: Missing values still exist after imputation!")

        # 2. Normalize trait scores (100-200 → 0-1)
        scaler = MinMaxScaler()
        df[feature_cols] = scaler.fit_transform(df[feature_cols])

        # 3. Train-test split (80/20)
        X_train, X_test, y_train, y_test = train_test_split(df[feature_cols], df[label_cols], test_size=0.2, random_state=42)

        # 4. Final NaN check after split
        if np.isnan(X_train.values).sum() > 0:
            print("NaNs detected in X_train after split! Applying final fillna.")
            X_train = pd.DataFrame(X_train).fillna(0).values  # Fallback imputation with zero

        # 5. Train Ridge Regression Models for Each Profile
        models = {}
        for class_label in label_cols:
            model = Ridge()
            model.fit(X_train, y_train[class_label])
            models[class_label] = model

        # Predict and Clip Probabilities
        y_pred_probs = pd.DataFrame({class_label: models[class_label].predict(X_test) for class_label in label_cols})
        y_pred_probs = y_pred_probs.clip(0, 1)

        # Evaluate with MSE
        mse_scores = {class_label: mean_squared_error(y_test[class_label], y_pred_probs[class_label]) for class_label in label_cols}

        # Print MSE Results
        print("\nMean Squared Error (MSE) for each profile:")
        for class_label, mse in mse_scores.items():
            print(f"{class_label}: {mse:.4f}")

        # Save models (optional)
        for class_label, model in models.items():
            joblib.dump(model, f"{class_label}_ridge_regression.pkl")

        print("\nRegression models trained and saved successfully!")


    def fetch_person_for_prediction(self, person_email):
        person = self.persons_repository.get_person_by_email(person_email)
        if not person:
            logger.info(f"Person {person_email} not found")
            return None
        profile_param_score = self.artifacts_service.calculate_overall_params(person.name, person.uuid)
        if not profile_param_score:
            logger.info(f"Profile param score not found for {person.name}")
            return None
        person_scores = {param: profile_param_score[param] if param in profile_param_score else np.nan for param in params} 
        # for param in params:
        #     if param not in profile_param_score:
        #         profile_param_score[param] = np.nan
        #     else:
        #         profile_param_score[param] = [profile_param_score[param]]
        return person_scores

    def predict_for_new_person(self, new_person_traits):
        """
        Predict probabilities for a new person based on their trait scores.
        :param new_person_traits: dict with keys as trait names and values as trait scores
        :return: dict with profile names and predicted probabilities
        """

        # Convert traits to DataFrame
        new_person_df = pd.DataFrame([new_person_traits])

        # Load saved mean values for missing traits
        means = joblib.load("trait_means.pkl")
        new_person_df = new_person_df.fillna(means)

        # Load and apply the saved scaler
        scaler = joblib.load("scaler.pkl")
        new_person_scaled = scaler.transform(new_person_df)

        # Load saved models for each profile
        profile_names = ["Innovator", "Go-Getter", "Analytical", "Social", "Emotional", "Thorough"]
        models = {profile: joblib.load(f"{profile}_ridge_regression.pkl") for profile in profile_names}

        # Predict probabilities and clip between 0 and 1
        predicted_probs = {profile: np.clip(models[profile].predict(new_person_scaled)[0], 0, 1) for profile in profile_names}

        # Display predicted probabilities
        print("\nPredicted Probabilities for New Person:")
        for profile, prob in predicted_probs.items():
            print(f"{profile}: {prob:.4f}")

        return predicted_probs



if __name__ == "__main__":
    profile_param_weights = ProfileParamWeights()
    profile_param_weights.prepare_data_for_training()
    logger.info("Training complete.")
    logger.info("Predicting for a new person...")
    person_scores = profile_param_weights.fetch_person_for_prediction("example@genieai.ai")
    profile_param_weights.predict_for_new_person(person_scores)