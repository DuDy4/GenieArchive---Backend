import datetime
from typing import Any, Dict, List
from collections import defaultdict

from pyarrow import timestamp

from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactScoreDTO, ArtifactSource, ArtifactType
from data.data_common.data_transfer_objects.news_data_dto import SocialMediaPost
from data.data_common.data_transfer_objects.work_history_dto import WorkHistoryArtifact
from data.data_common.dependencies.dependencies import artifacts_repository, artifact_scores_repository
from data.data_common.services.profile_params_service import ProfileParamsService
from common.genie_logger import GenieLogger

logger = GenieLogger()


class ArtifactsService():

    def __init__(self):
        self.artifacts_repository = artifacts_repository()
        self.artifact_scores_repository = artifact_scores_repository()
        self.profile_params_service = ProfileParamsService()    

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
    

    async def calculate_artifact_scores(self, artifact: ArtifactDTO | WorkHistoryArtifact, person):
        """
        Calculate scores for artifact
        :param artifact: Artifact to calculate scores for
        """
        # timestamp = datetime.datetime.now()
        logger.info(f"Calculating scores for artifact {artifact.uuid}")
        param_scores = await self.profile_params_service.evaluate_all_params(artifact.text, person.name,
                                                                             person.position, person.company, isinstance(artifact, WorkHistoryArtifact))
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
        for param, avg_score in param_averages.items():
            logger.info(f"{param}: {avg_score}")
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