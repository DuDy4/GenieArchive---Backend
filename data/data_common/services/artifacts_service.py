import datetime
from typing import Any, Dict, List
from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactScoreDTO, ArtifactSource, ArtifactType
from data.data_common.dependencies.dependencies import artifact_repository, artifact_scores_repository
from data.data_common.services.profile_params_service import ProfileParamsService
from common.genie_logger import GenieLogger

logger = GenieLogger()


class ArtifactSerivce():

    def __init__(self):
        self.artifact_repository = artifact_repository()
        self.artifact_scores_repository = artifact_scores_repository()
        self.profile_params_service = ProfileParamsService()    

    def save_linkedin_posts(self, profile_uuid, posts: List[Dict[str, Any]]) -> List[str]:
        """
        Save linkedin posts to database
        :param posts: List of posts
        :return: List of UUIDs of saved posts
        """
        uuids = []
        for post in posts:
            artifact = ArtifactDTO.from_dict(post)
            artifact.source = ArtifactSource.LINKEDIN
            artifact.artifact_type = ArtifactType.POST
            artifact.profile_uuid = profile_uuid
            uuid = self.artifact_repository.save_artifact(artifact)
            uuids.append(uuid)
        return uuids
    
    def get_self_written_posts(self, profile_uuid: str, linkedin_url: str) -> List[ArtifactDTO]:
        """
        Get self written posts
        :param profile_uuid: Profile UUID
        :return: List of self written posts
        """
        all_posts =  self.artifact_repository.get_user_artifacts(profile_uuid, ArtifactType.POST)
        self_written_posts = []
        for post in all_posts:
            if post.source == ArtifactSource.LINKEDIN and post.metadata and post.metadata.get('reshared') and post.metadata.get('reshared').contains(linkedin_url):
                self_written_posts.append(post)
        return self_written_posts
    
    def get_artifact(self, artifact_uuid) -> ArtifactDTO:
        """
        Get artifact by UUID
        :param artifact_uuid: UUID of artifact
        :return: Artifact
        """
        return self.artifact_repository.get_artifact(artifact_uuid)
    

    async def calculate_artifact_scores(self, artifact: ArtifactDTO, person):
        """
        Calculate scores for artifact
        :param artifact: Artifact to calculate scores for
        """
        timestamp = datetime.datetime.now()
        logger.info(f"Calculating scores for artifact {artifact.uuid}")
        param_scores = await self.profile_params_service.evaluate_all_params(artifact.text, person.name, person.position, person.company)
        logger.info(f"Calculated scores for artifact {artifact.uuid}. Duration: {datetime.datetime.now() - timestamp} ms")
        self.artifact_scores_repository.upsert_artifact_scores(param_scores)