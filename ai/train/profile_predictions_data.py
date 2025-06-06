import math

from common.genie_logger import GenieLogger
import pandas as pd

logger = GenieLogger()


class ProfilePredictionsData:

    def __init__(self):
        self.profiles = ["Innovator", "Go-Getter", "Analytical", "Social", "Intuitive", "Thorough"]
        self.w_top=2
        self.w_second=1
        self.w_last=-1
        self.alpha=0.4
        self.center=1.0
        self.predictions = self.gather_predictions()

    def gather_predictions(self):
        raw_data = """
            Adi Baltter,Innovator,Social,Analytical,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Innovator,Analytical,Thorough,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Go-Getter,Innovator,Intuitive,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Innovator,Go-Getter,Analytical,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Innovator,Analytical,Social,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Social,Innovator,Intuitive,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Analytical,Innovator,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Innovator,Analytical,Intuitive,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Analytical,Innovator,Go-Getter,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Analytical,Innovator,Thorough,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Analytical,Go-Getter,Intuitive,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Go-Getter,Innovator,Analytical,https://www.linkedin.com/in/adibaltter/
            Adi Baltter,Social,Innovator,Go-Getter,
            Alon Stern,Social,Analytical,Innovator,https://www.linkedin.com/in/alon-stern-121766125/
            Alon Stern,Intuitive,Thorough,Innovator,https://www.linkedin.com/in/alon-stern-121766125/
            Alon Stern,Thorough,Social,Innovator,https://www.linkedin.com/in/alon-stern-121766125/
            Alon Stern,Analytical,Social,Intuitive,https://www.linkedin.com/in/alon-stern-121766125/
            Alon Stern,Go-Getter,Social,Intuitive,https://www.linkedin.com/in/alon-stern-121766125/
            Amit Greenberg,Analytical,Thorough,Intuitive,https://www.linkedin.com/in/amit-greenberg-3639233/
            Amit Zohar,Analytical,Thorough,Intuitive,https://www.linkedin.com/in/a-zohar/
            Amit Zohar,Analytical,Innovator,Social,https://www.linkedin.com/in/a-zohar/
            Amit Zohar,Analytical,Innovator,Thorough,https://www.linkedin.com/in/a-zohar/
            Asaf Savich,Innovator,Analytical,Go-Getter,https://www.linkedin.com/in/asaf-savich/
            Asaf Savich,Social,Go-Getter,Thorough,https://www.linkedin.com/in/asaf-savich/
            Asaf savich,Thorough,Innovator,Intuitive,https://www.linkedin.com/in/asaf-savich/
            Asaf Savich,Analytical,Go-Getter,Intuitive,https://www.linkedin.com/in/asaf-savich/
            Asaf Savich,Go-Getter,Innovator,Intuitive,https://www.linkedin.com/in/asaf-savich/
            Asaf Savich,Social,Innovator,Intuitive,https://www.linkedin.com/in/asaf-savich/
            Avi Cohen,Innovator,Go-Getter,Thorough,
            Dafna Greenshpan ,Thorough,Analytical,Social,
            Danna Regev,Thorough,Analytical,Go-Getter,
            Danna Regev,Analytical,Intuitive,Innovator,https://www.linkedin.com/in/danna-regev-5612662/
            Danna Regev,Analytical,Intuitive,Innovator,https://www.linkedin.com/in/danna-regev-5612662/
            Efrat Greenberg,Go-Getter,Analytical,Intuitive,https://www.linkedin.com/in/efratgreenberg/
            Eyal Yona,Social,Analytical,Thorough,https://www.linkedin.com/in/eyal-yona/
            Eyal Yona,Innovator,Social,Analytical,https://www.linkedin.com/in/eyal-yona/
            Eyal Yona,Go-Getter,Analytical,Thorough,https://www.linkedin.com/in/eyal-yona/
            Eyal Yona,Analytical,Innovator,Thorough,https://www.linkedin.com/in/eyal-yona/
            Gal rettig ,Analytical,Social,Thorough,https://www.linkedin.com/in/gal-rettig-717332a1/
            Gal Walters,Social,Analytical,Innovator,https://www.linkedin.com/in/gal-walters-4a6b83233/
            Gal Walters,Go-Getter,Innovator,Intuitive,https://www.linkedin.com/in/gal-walters-4a6b83233/
            Gal Walters,Innovator,Analytical,Thorough,https://www.linkedin.com/in/gal-walters-4a6b83233/
            Golan Raz,Innovator,Go-Getter,Analytical,
            Hadas shay,Social,Intuitive,Go-Getter,
            Hilla katzir,Analytical,Thorough,Innovator,
            Ilana Milshtain,Go-Getter,Analytical,Intuitive,https://www.linkedin.com/in/ilana-milishtein-4b86b5226/
            Ilana Milshtain,Social,Go-Getter,Thorough,https://www.linkedin.com/in/ilana-milishtein-4b86b5226/
            Ilana Milshtain,Analytical,Social,Thorough,https://www.linkedin.com/in/ilana-milishtein-4b86b5226/
            Itai Shachar,Thorough,Analytical,Intuitive,https://www.linkedin.com/in/itai-shachar-8670b5111/
            Itai Shachar,Social,Thorough,Analytical,https://www.linkedin.com/in/itai-shachar-8670b5111/
            Itai Shachar,Analytical,Intuitive,Social,https://www.linkedin.com/in/itai-shachar-8670b5111/
            Itai Shachar,Thorough,Analytical,Go-Getter,https://www.linkedin.com/in/itai-shachar-8670b5111/
            Itai Shachar,Social,Intuitive,Innovator,https://www.linkedin.com/in/itai-shachar-8670b5111/
            Itay Grushka,Go-Getter,Analytical,Thorough,
            Jonatan Darr,Analytical,Innovator,Intuitive,
            Jonathan Barazany,Thorough,Analytical,Intuitive,https://www.linkedin.com/in/jonathan-barazany-14925849/
            Jonathan Barazany,Analytical,Go-Getter,Intuitive,https://www.linkedin.com/in/jonathan-barazany-14925849/
            Joseph Haskin,Thorough,Social,Go-Getter,https://www.linkedin.com/in/sefi-haskin/
            Kim Berger,Social,Thorough,Go-Getter,https://www.linkedin.com/in/kim-berger-94684018b/
            Kim Berger,Social,Go-Getter,Analytical,https://www.linkedin.com/in/kim-berger-94684018b/
            Kim Berger,Social,Intuitive,Analytical,https://www.linkedin.com/in/kim-berger-94684018b/
            Kim Berger,Innovator,Thorough,Intuitive,https://www.linkedin.com/in/kim-berger-94684018b/
            Liat Egel,Social,Go-Getter,Thorough,
            Linoy Ravia,Go-Getter,Analytical,Social,https://www.linkedin.com/in/linoy-ravia/
            Linoy Ravia,Social,Thorough,Analytical,https://www.linkedin.com/in/linoy-ravia/
            Linoy Ravia,Analytical,Intuitive,Social,https://www.linkedin.com/in/linoy-ravia/
            Menashe Haskin,Innovator,Analytical,Intuitive,https://www.linkedin.com/in/menashe-haskin/
            Michal Cohen,Intuitive,Thorough,Analytical,https://www.linkedin.com/in/michal-cohen-972ba232b/
            Michal Cohen,Intuitive,Social,Analytical,https://www.linkedin.com/in/michal-cohen-972ba232b/
            Michal Cohen,Intuitive,Go-Getter,Social,https://www.linkedin.com/in/michal-cohen-972ba232b/
            Michal Cohen,Social,Intuitive,Analytical,https://www.linkedin.com/in/michal-cohen-972ba232b/
            Michal Shor,Social,Intuitive,Analytical,https://www.linkedin.com/in/michal-kunya-shor-107a0b1b1/
            Michal Shor,Innovator,Thorough,Social,https://www.linkedin.com/in/michal-kunya-shor-107a0b1b1/
            Michal Shor,Analytical,Social,Go-Getter,https://www.linkedin.com/in/michal-kunya-shor-107a0b1b1/
            Chen ,Thorough,Analytical,Social,
            Nadin Rafael,Thorough,Intuitive,Innovator,-
            Nadin Rafael,Go-Getter,Analytical,Social,-
            Nadin Rafael,Innovator,Intuitive,Analytical,-
            Natalie Friedman,Go-Getter,Social,Intuitive,
            Niv Baltter,Innovator,Analytical,Social,https://www.linkedin.com/in/nivb/
            Niv Baltter,Analytical,Social,Innovator,https://www.linkedin.com/in/nivb/
            Nofar Tsur,Intuitive,Social,Analytical,https://www.linkedin.com/in/nofartsur/
            Nofar Tsur,Intuitive,Analytical,Innovator,https://www.linkedin.com/in/nofartsur/
            Nofar Tsur,Go-Getter,Intuitive,Thorough,https://www.linkedin.com/in/nofartsur/
            Nurit Zimmermann-Haskin,Social,Innovator,Analytical,https://www.linkedin.com/in/nurit-zimmermann-haskin/
            Opal Balter,Intuitive,Social,Analytical,https://www.linkedin.com/in/opal-balter-606079289/
            Opal Balter,Intuitive,Social,Analytical,https://www.linkedin.com/in/opal-balter-606079289/
            Opal Balter,Social,Intuitive,Analytical,https://www.linkedin.com/in/opal-balter-606079289/
            Rani,Social,Innovator,Intuitive,
            Reef Baltter,Analytical,Go-Getter,Intuitive,https://www.linkedin.com/in/reefb/
            Reef Baltter,Analytical,Thorough,Social,https://www.linkedin.com/in/reefb/
            Reef Baltter,Analytical,Innovator,Intuitive,https://www.linkedin.com/in/reefb/
            Reef Baltter,Intuitive,Go-Getter,Analytical,https://www.linkedin.com/in/reefb/
            Reef Baltter,Analytical,Innovator,Intuitive,https://www.linkedin.com/in/reefb/
            Reef Baltter,Analytical,Social,Thorough,https://www.linkedin.com/in/reefb/
            Sari Menaker,Social,Analytical,Innovator,
            Asaf Savich,Social,Innovator,Thorough,https://www.linkedin.com/in/asaf-savich/
            Shai Yagil,Innovator,Thorough,Intuitive,https://www.linkedin.com/in/shai-yagil-1b1004b9/
            Shai Yagil,Thorough,Go-Getter,Intuitive,https://www.linkedin.com/in/shai-yagil-1b1004b9/
            Shai Yagil,Analytical,Thorough,Innovator,https://www.linkedin.com/in/shai-yagil-1b1004b9/
            Shai Yagil,Thorough,Analytical,Intuitive,https://www.linkedin.com/in/shai-yagil-1b1004b9/
            Shai Yagil,Thorough,Analytical,Intuitive,https://www.linkedin.com/in/shai-yagil-1b1004b9/
            Shiraz Amit,Thorough,Analytical,Social,https://www.linkedin.com/in/amitshiraz/
            Shiraz Amit,Social,Analytical,Innovator,https://www.linkedin.com/in/amitshiraz/
            Shiraz Amit,Social,Thorough,Analytical,https://www.linkedin.com/in/amitshiraz/
            Shiraz Amit,Social,Intuitive,Go-Getter,https://www.linkedin.com/in/amitshiraz/
            Stav Eliezer,Thorough,Go-Getter,Innovator,-
            Stav Eliezer,Social,Thorough,Innovator,-
            Stav Eliezer,Analytical,Thorough,Innovator,-
            Tal Schachter ,Social,Thorough,Innovator,https://www.linkedin.com/in/tal-schachter-328a46a1/
            Tal Schachter ,Analytical,Social,Intuitive,https://www.linkedin.com/in/tal-schachter-328a46a1/
            Tal Zuriel,Social,Innovator,Analytical,https://www.linkedin.com/in/talzuriel/
            Tomer Katzav,Social,Analytical,Go-Getter,https://www.linkedin.com/in/tomer-katzav-8158a769/
            Tomer Katzav,Social,Intuitive,Innovator,https://www.linkedin.com/in/tomer-katzav-8158a769/
            Tomer Katzav,Intuitive,Thorough,Innovator,https://www.linkedin.com/in/tomer-katzav-8158a769/
            Tomer Katzav,Social,Go-Getter,Innovator,https://www.linkedin.com/in/tomer-katzav-8158a769/
            Tomer Katzav ,Social,Intuitive,Go-Getter,https://www.linkedin.com/in/tomer-katzav-8158a769/
            Tomer Weiss,Analytical,Thorough,Intuitive,-
            Tomer Weiss,Analytical,Thorough,Intuitive,-
            Tzili golan,Social,Thorough,Intuitive,
            Yaron Regev,Go-Getter,Analytical,Intuitive,https://www.linkedin.com/in/yaronregev/
            Yaron Regev ,Thorough,Analytical,Intuitive,https://www.linkedin.com/in/yaronregev/
            Yonatan Shor,Thorough,Analytical,Innovator,https://www.linkedin.com/in/yonatan-shor/
            Yonatan Shor,Thorough,Analytical,Innovator,https://www.linkedin.com/in/yonatan-shor/
            Yonatan Shor,Analytical,Social,Go-Getter,https://www.linkedin.com/in/yonatan-shor/
            Yotam tzafrir,Analytical,Thorough,Intuitive,
            """.strip().splitlines()

        """Adi Baltter,Emotional,Social,Analytical Adi Baltter,Go-Getter,Analytical,Innovator Adi Baltter,Innovator,Go-Getter,Analytical Adi Baltter,Innovator,Thorough,Social Adi Baltter,Social,Emotional,Innovator   
amiri,Analytical,Go-Getter,Emotional amiri,Analytical,Thorough,Emotional amiri,Go-Getter,Thorough,Emotional amiri,Go-Getter,Thorough,Innovator   
Boaz,Analytical,Go-Getter,Emotional Boaz,Go-Getter,Innovator,Thorough Boaz,Innovator,Analytical,Emotional   
Danny,Innovator,Analytical,Emotional Danny,Innovator,Emotional,Go-Getter Danny,Innovator,Go-Getter,Social   
Dror,Analytical,Social,Thorough Dror,Go-Getter,Innovator,Analytical Dror,Innovator,Social,Thorough   
Gal,Analytical,Go-Getter,Emotional Gal,Social,Emotional,Analytical Gal,Social,Thorough,Emotional Gal,Thorough,Social,Analytical Gal,Thorough,Social,Innovator   
Guy,Analytical,Innovator,Emotional Guy,Go-Getter,Analytical,Innovator Guy,Go-Getter,Innovator,Emotional   
Oded,Analytical,Go-Getter,Emotional Oded,Analytical,Go-Getter,Innovator Oded,Analytical,Innovator,Emotional Oded,Analytical,Thorough,Emotional   
Shay,Analytical,Thorough,Emotional Shay,Social,Analytical,Emotional Shay,Social,Emotional,Thorough Shay,Social,Go-Getter,Analytical Shay,Thorough,Social,Innovator   
Shimi,Analytical,Social,Emotional Shimi,Go-Getter,Social,Emotional Shimi,Thorough,Analytical,Emotional Shimi,Thorough,Social,Innovator   
Tomer M,Analytical,Thorough,Innovator Tomer M,Emotional,Social,Innovator Tomer M,Social,Thorough,Emotional Tomer M,Thorough,Emotional,Social   
Yaniv,Analytical,Go-Getter,Go-Getter Yaniv,Analytical,Social,Thorough Yaniv,Analytical,Thorough,Innovator Yaniv,Go-Getter,Emotional,Analytical Yaniv,Go-Getter,Thorough,Innovator Yaniv,Thorough,Analytical,Innovator   
Yeftach,Analytical,Social,Innovator Yeftach,Analytical,Thorough,Social Yeftach,Social,Innovator,Go-Getter Yeftach,Thorough,Emotional,Emotional   """

        guesses_dict = {}

        for line in raw_data:
            # Split by comma
            parts = [p.strip() for p in line.split(",")]
            
            # Expect at least 4 columns (Name, First, Second, Least), 5th is LinkedIn (optional).
            # Some lines have missing columns, so we handle that carefully:
            name       = parts[0] if len(parts) > 0 else ""
            first      = parts[1] if len(parts) > 1 else ""
            second     = parts[2] if len(parts) > 2 else ""
            least      = parts[3] if len(parts) > 3 else ""
            linkedin   = parts[4] if len(parts) > 4 else ""
            
            # Initialize this name in the dictionary if not present
            if name not in guesses_dict:
                guesses_dict[name] = []
            
            # Append the guess object
            guesses_dict[name].append({
                "first": first,
                "second": second,
                "least": least,
                "linkedin": linkedin
            })

        all_persons_probabilities = {}
        for person, guesses in guesses_dict.items():
            if len(guesses) > 1:
                person_profile_probabilities = self.calculate_probabilities(guesses)
                if person_profile_probabilities:
                    all_persons_probabilities[person] = person_profile_probabilities
                    print(f"Probabilities: {person_profile_probabilities} | {person}")
                else:
                    logger.error(f"Failed to calculate probabilities for {person}")

        return all_persons_probabilities



    def calculate_probabilities(self, guesses):
        """
        Calculate a probability distribution for the given guesses.
        
        :param guesses: A list of tuples like (top, second, least).
        :return: A dict {profile: probability}.
        """
        
        # Initialize scores
        scores = {profile: 0.0 for profile in self.profiles}
        
        # Tally scores based on guesses
        for guess in guesses:
            if guess['first'] in scores:
                scores[guess['first']] += self.w_top
            if guess['second'] in scores:
                scores[guess['second']] += self.w_second
            if guess['least'] in scores:
                scores[guess['least']] += self.w_last

        memberships = {}
        for profile, raw_score in scores.items():
            memberships[profile] = self._logistic(raw_score)
        
        return memberships


    def _logistic(self, x):
        """
        Logistic (sigmoid) transform of x using alpha and center.
        Returns a value in (0,1).
        """
        return 1.0 / (1.0 + math.exp(-self.alpha * (x - self.center)))




def excel_to_dict_list(file_path):
    # Load the Excel file
    df = pd.read_excel(file_path)

    # Initialize an empty list to store the dictionaries
    dict_list = []

    # Slice out the first two columns (date and names)
    date_and_names = df.iloc[:, 1]  # Get the "name" column (index 1)
    data = df.iloc[:, 2:]  # Slice out everything starting from the 3rd column (index 2)

    # Iterate through every 4 columns
    for i in range(0, data.shape[1], 4):  # Step by 4 columns
        group = data.iloc[:, i:i+4]  # Get the current group of 4 columns

        # Iterate over each row in the group
        for idx, row in group.iterrows():
            # Ensure no empty rows are processed
            if row.notna().all():  # Skip rows with NaN values
                # Create a dictionary for the current profile
                profile_dict = {
                    "name": row.iloc[0],
                    "most_likely": row.iloc[1],
                    "second_likely": row.iloc[2],
                    "least_likely": row.iloc[3]
                }
                dict_list.append(profile_dict)
    dict_list_sorted = sorted(dict_list, key=lambda x: x['name'])
    return dict_list_sorted

# Example Usage
file_path = "Genie_Profiling_Form2025-02-19_03_22_49.xlsx"  # Replace with your Excel file path
profiles = excel_to_dict_list(file_path)

# Print the results
for profile in profiles:
    print(profile)

