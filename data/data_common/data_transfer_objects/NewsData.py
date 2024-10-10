import json
from typing import List, Dict, Optional, Union, Tuple, Any
from pydantic import HttpUrl, field_validator, BaseModel, ValidationError, Field
from datetime import date

from data.data_common.utils.str_utils import (
    titleize_values,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()


class NewsData(BaseModel):
    date: Optional[date]
    link: HttpUrl
    media: str
    title: str
    summary: Optional[str]

    @field_validator("media", "title", "link")
    def not_empty(cls, value):
        # Convert HttpUrl to str if needed, otherwise ensure it's a string
        value_to_check = str(value)

        # Check if the value is empty or whitespace
        if not value_to_check.strip():
            raise ValueError("Field cannot be empty or whitespace")

        return value

    @classmethod
    def from_json(cls, json_str: str) -> "NewsData":
        return cls.parse_raw(json_str)

    def to_json(self) -> str:
        return self.json()

    def to_tuple(self) -> Tuple[Optional[date], HttpUrl, str, str, Optional[str]]:  # type: ignore
        return self.date, self.link, self.media, self.title, self.summary

    @classmethod
    def from_tuple(cls, data: Tuple[Optional["date"], str, str, str, Optional[str]]) -> "NewsData":
        return cls(date=data[0], link=data[1], media=data[2], title=data[3], summary=data[4])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "link": str(self.link),
            "media": self.media,
            "title": self.title,
            "summary": self.summary,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "NewsData":
        data = titleize_values(data)
        return cls(
            date=data.get("date"),
            link=data.get("link"),
            media=data.get("media"),
            title=data.get("title"),
            summary=data.get("summary"),
        )

    def process_news(news: List[dict]) -> List:
        logger.debug(f"News data: {news}")
        res_news = []
        if news:
            for item in news:
                logger.debug(f"Item: {item}")
                try:
                    deserialized_news = NewsData.from_dict(item)
                    logger.debug(f"Deserialized news: {deserialized_news}")
                    if deserialized_news:
                        res_news.append(deserialized_news)
                    logger.debug(f"Processed news: {res_news}")
                except Exception as e:
                    logger.error(f"Error deserializing news: {e}. Skipping this news item")
        logger.debug(f"News data: {res_news}")
        return res_news


class SocialMediaPost(NewsData):
    reshared: bool = Field(default=False, description="Indicates if the post is reshared")
    likes: int = Field(default=0, description="Number of likes on the post")
    images: Optional[List[HttpUrl]] = Field(
        default=None, description="List of image URLs associated with the post"
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """
        Create a SocialMediaPost instance from a dictionary.
        This method uses the same fields as NewsData and extends it with SocialMediaPost-specific fields.
        """
        try:
            return cls(**data)
        except ValidationError as e:
            logger.error(f"Validation error while creating SocialMediaPost: {e}")
            return None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the SocialMediaPost instance to a dictionary, ensuring all fields are included.
        """
        # Use the Pydantic's dict() method to ensure all inherited fields are included
        base_dict = super().to_dict()
        # Add additional fields from SocialMediaPost
        base_dict.update({"reshared": self.reshared, "likes": self.likes, "images": self.images})
        return base_dict

    def __str__(self):
        """
        Custom string representation of the SocialMediaPost object, including inherited fields.
        """
        # Use the base class's __str__ method and extend it
        return f"SocialMediaPost({super().__str__()}, reshared={self.reshared}, likes={self.likes}, images={self.images})"

    def __repr__(self):
        """
        Custom representation of the SocialMediaPost object, including inherited fields.
        """
        return f"SocialMediaPost({super().__repr__()}, reshared={self.reshared}, likes={self.likes}, images={self.images})"


# Example usage for testing
if __name__ == "__main__":
    # Sample data for testing
    sample_data = {
        "date": "2024-09-25",
        "link": "https://www.linkedin.com/feed/update/urn:li:activity:7244850178187251712/",
        "media": "LinkedIn",
        "title": "Example Post",
        "summary": "This is a sample post summary",
        "image_urls": ["https://example.com/image1.jpg", "https://example.com/image2.jpg"],
        "reshared": True,
        "likes": 100,
        "images": ["https://example.com/image1.jpg"],
    }

    # Create an instance from the sample data
    social_media_post = SocialMediaPost.from_dict(sample_data)
    print(social_media_post)

    # Convert the instance back to a dictionary
    print(social_media_post.to_dict())
