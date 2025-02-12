import datetime
from typing import Any, Dict, List
from collections import defaultdict

from pyarrow import timestamp

from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactScoreDTO, ArtifactSource, ArtifactType
from data.data_common.data_transfer_objects.news_data_dto import SocialMediaPost
from data.data_common.data_transfer_objects.work_history_dto import WorkHistoryArtifactDTO
from data.data_common.repositories.artifact_scores_repository import ArtifactScoresRepository
from data.data_common.repositories.artifacts_repository import ArtifactsRepository
from data.data_common.services.profile_params_service import ProfileParamsService
from ai.train.profile_param_weights import ProfileParamWeights
from common.genie_logger import GenieLogger

logger = GenieLogger()
profile_model_trained = False


class ArtifactsService():

    def __init__(self):
        global profile_model_trained
        self.artifacts_repository = ArtifactsRepository()
        self.artifact_scores_repository = ArtifactScoresRepository()
        self.profile_params_service = ProfileParamsService()  
        self.profile_param_weights = ProfileParamWeights()  

        if not profile_model_trained:
            profile_model_trained = True
            self.prepare_data_for_training()


    def prepare_data_for_training(self):
        unique_profile_score_dicts = []
        unique_profile_dicts = self.get_unique_profiles()
        for profile_dict in unique_profile_dicts:
            profile_name = profile_dict.get("name")
            profile_uuid = profile_dict.get("uuid")
            profile_param_score = self.calculate_overall_params(profile_name, profile_uuid)
            if not profile_param_score:
                continue
            unique_profile_score_dicts.append({"name": profile_name, "scores": profile_param_score})
        self.profile_param_weights.prepare_data_for_training2(unique_profile_score_dicts)

        
    def save_linkedin_posts(self, profile_uuid, posts: List[SocialMediaPost]) -> List[str]:
        """
        Save linkedin posts to database
        :param posts: List of posts
        :return: List of UUIDs of saved posts
        """
        uuids = []
        for post in posts:
            artifact = ArtifactDTO.from_social_media_post(post, profile_uuid)
            artifact.source = ArtifactSource.LINKEDIN
            artifact.artifact_type = ArtifactType.POST
            uuid = self.artifacts_repository.save_artifact(artifact)
            uuids.append(uuid)
        return uuids
    
    def get_self_written_posts(self, profile_uuid: str, linkedin_url: str) -> List[ArtifactDTO]:
        """
        Get self written posts
        :param profile_uuid: Profile UUID
        :return: List of self written posts
        """
        all_posts =  self.artifacts_repository.get_user_artifacts(profile_uuid, ArtifactType.POST)
        self_written_posts = []
        for post in all_posts:
            if post.source == ArtifactSource.LINKEDIN and post.metadata and post.metadata.get('reshared') \
            and linkedin_url in post.metadata.get('reshared'):
                self_written_posts.append(post)
        self_written_posts = sorted(self_written_posts, key=lambda x: x.created_at, reverse=True)[:10]  # Only get the latest 10 posts for testing purposes
        return self_written_posts
    
    def get_artifact(self, artifact_uuid) -> ArtifactDTO:
        """
        Get artifact by UUID
        :param artifact_uuid: UUID of artifact
        :return: Artifact
        """
        return self.artifacts_repository.get_artifact(artifact_uuid)
    

    async def calculate_artifact_scores(self, artifact: ArtifactDTO | WorkHistoryArtifactDTO, person, isWorkHistory=False):
        """
        Calculate scores for artifact
        :param artifact: Artifact to calculate scores for
        """
        # timestamp = datetime.datetime.now()
        logger.info(f"Calculating scores for artifact {artifact.uuid}")
        if isWorkHistory:
            param_scores = await self.profile_params_service.evaluate_work_history_params(artifact.to_dict(), person.name,
                                                                                          person.position, person.company)
        else:
            param_scores = await self.profile_params_service.evaluate_all_params(artifact.text, person.name,
                                                                                person.position, person.company)
        param_scores_to_persist = []
        for param_score in param_scores:
            if param_score.get("score"):
                score_dto = ArtifactScoreDTO.from_evaluation_dict(artifact.uuid, param_score)
                param_scores_to_persist.append(score_dto)
        self.artifact_scores_repository.upsert_artifact_scores(param_scores_to_persist)


    def calculate_overall_params(self, name, profile_uuid):
        """
        Calculate overall params for profile
        :param profile_uuid: UUID of profile
        """
        timestamp = datetime.datetime.now()
        logger.info(f"Calculating overall params for person {name} | {profile_uuid}")
        # artifacts = self.artifacts_repository.get_user_artifacts(profile_uuid)
        # if not artifacts:
        #     return {}
        # all_artifacts_scores = []
        # for artifact in artifacts:
        #     artifact_scores = self.artifact_scores_repository.get_artifact_scores_by_artifact_uuid(artifact.uuid)
        #     artifact_weight = 1 # Placeholder for Mazgan
        #     all_artifacts_scores.extend(artifact_scores)
        all_artifacts_scores = self.artifact_scores_repository.get_all_artifact_scores_by_profile_uuid(profile_uuid)
        param_averages = self.calculate_average_scores_per_param(all_artifacts_scores)
        # for param, avg_score in param_averages.items():
            # logger.info(f"{param}: {avg_score}")
        logger.info(f"Calculated overall params for profile {name}. Duration: {datetime.datetime.now() - timestamp} ms")
        logger.info(f"Overall params: {param_averages}")
        return param_averages
    
    
    def calculate_average_scores_per_param(self, artifact_scores: List[ArtifactScoreDTO]) -> Dict[str, float]:
        """
        Calculate the average score per parameter from a list of ArtifactScoreDTO objects.

        :param artifact_scores: List of ArtifactScoreDTO objects
        :return: A dictionary with parameters as keys and their average scores as values
        """
        score_sums = defaultdict(int)  # To store the sum of scores for each param
        score_counts = defaultdict(int)  # To store the count of scores for each param

        for artifact_score in artifact_scores:
            param = artifact_score.param
            score = artifact_score.score
            score_sums[param] += score
            score_counts[param] += 1

        # Calculate averages
        average_scores = {
            param: score_sums[param] / score_counts[param]
            for param in score_sums
        }

        return average_scores

    def check_existing_artifact(self, artifact):
        has_artifact = self.artifacts_repository.check_existing_artifact(artifact)
        has_score = self.artifact_scores_repository.exists(artifact.uuid)
        return has_artifact and has_score

    def exists_work_history_element(self, work_history_artifact):
        artifact_uuid = self.artifacts_repository.exists_work_history_element(work_history_artifact)
        has_score = self.artifact_scores_repository.exists_for_artifact(artifact_uuid)
        return artifact_uuid is not None and has_score
    
    def get_unique_profiles(self):
        """
        Get unique profiles
        :return: List of unique profiles
        """
        return self.artifacts_repository.get_unique_users()