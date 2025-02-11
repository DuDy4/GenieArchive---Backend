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
        self.artifacts_service = ArtifactsService()
        self.profile_predictions_data = ProfilePredictionsData()
        self.predictions = self.profile_predictions_data.predictions
        self.people_anaylsed = []

        self.data = {}
        for param in params:
            self.data[param] = []

        self.prepare_data_for_training()


    def prepare_data_for_training(self):
        unique_profile_dicts = self.artifacts_service.get_unique_profiles()
        profiles_param_scores = {}
        people = []
        for profile_dict in unique_profile_dicts:
            profile_name = profile_dict.get("name")
            profile_uuid = profile_dict.get("uuid")
            profile_param_score = self.artifacts_service.calculate_overall_params(profile_name, profile_uuid)
            if not profile_param_score:
                continue
            if self.predictions.get(profile_name) is None:
                logger.info(f"No predictions found for person {profile_name}")
                continue
            people.append({'name' : profile_name, 'traits' : profile_param_score, 'profiles' : self.predictions[profile_name]})
            profiles_param_scores[profile_uuid] = profile_param_score
            self.people_anaylsed.append(profile_name)

        if people:
            training_data = self.create_data_dictionary(people)

        for prediction in self.predictions:
            if prediction not in self.people_anaylsed:
                logger.info(f"Person {prediction} not found in the data")

        self.train(training_data)

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

    def train(self, data):
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
    

    def normalize_param_scores(self, profile_param_score):
        person_scores = {param: profile_param_score[param] if param in profile_param_score else np.nan for param in params} 
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
    # person_scores = profile_param_weights.fetch_person_for_prediction("amit.svarzenberg@microsoft.com")
    logger.info("Predicting for a new person...")
    profile_param_weights.predict_for_new_person(person_scores)
    logger.info("Prediction complete.")