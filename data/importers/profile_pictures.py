import os, sys, re, requests

from bs4 import BeautifulSoup
from linkedin_profile_picture import ProfilePicture

from data.data_common.data_transfer_objects.company_dto import SocialMediaLinks

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from urllib.parse import urlparse, unquote, urlunparse
from dotenv import load_dotenv, find_dotenv

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from common.genie_logger import GenieLogger

load_dotenv(find_dotenv())

logger = GenieLogger()


class LinkedInProfilePictureFetcher:
    def __init__(self):
        self.profile_picture = ProfilePicture(
            os.environ.get("GOOGLE_DEVELOPER_API_KEY"), os.environ.get("GOOGLE_CX")
        )

    def check_picture_url(self, link: str) -> bool:
        match = re.findall(r"(\.licdn\.com).+?(profile-displayphoto-shrink_)", link)
        return bool(match)

    def check_url_exists(self, link):
        flag = False
        try:
            resp = requests.get(link, timeout=5)
            if resp and resp.status_code == 200:
                flag = True
        except:
            pass
        return flag

    def extract_profile_picture(self, res: list) -> str:
        link = ""
        for i in res:
            metatags = i.get("pagemap", {}).get("metatags", [])
            metatags = sum(
                list(
                    map(
                        lambda mt: list(dict(filter(lambda x: "image" in x[1], mt.items())).values()),
                        metatags,
                    )
                ),
                [],
            )

            # cse_imgs = i.get("pagemap",{}).get("cse_image", [])
            # cse_imgs = list(filter(None, map(lambda x:x.get("src"), cse_imgs)))

            # pic_urls = set(metatags + cse_imgs)
            pic_urls = set(metatags)
            for url in pic_urls:
                if self.check_picture_url(url) and self.check_url_exists(url):
                    link = url
                    break
            if link:
                break
        return link

    def fix_linkedin_url(self, linkedin_url: str) -> str:
        """
        Converts a full LinkedIn URL to a shortened URL.

        Args:
            linkedin_url (str): The full LinkedIn URL.

        Returns:
            str: The shortened URL.
        """
        linkedin_url = linkedin_url.replace("http://www.linkedin.com/in/", "linkedin.com/in/")
        linkedin_url = linkedin_url.replace("https://www.linkedin.com/in/", "linkedin.com/in/")
        linkedin_url = linkedin_url.replace("http://linkedin.com/in/", "linkedin.com/in/")
        linkedin_url = linkedin_url.replace("https://linkedin.com/in/", "linkedin.com/in/")

        if linkedin_url[-1] == "/":
            linkedin_url = linkedin_url[:-1:]
        return linkedin_url

    def get_profile_picture_from_linkedin_url(self, linkedin_url: str) -> str:
        """
        Extracts the profile picture URL from a LinkedIn URL.

        Args:
            linkedin_url (str): The LinkedIn URL.

        Returns:
            str: The profile picture URL.
        """
        if linkedin_url:
            linkedin_id = self.fix_linkedin_url(linkedin_url)
            res = self.profile_picture.search(linkedin_id)
            image_link = self.extract_profile_picture(res._search_results)
            return image_link
        logger.warning("No LinkedIn URL provided.")


class ProfilePictureFetcher:
    def __init__(self):
        self.linkedin_profile_picture_fetcher = LinkedInProfilePictureFetcher()

    def get_profile_picture_from_person(self, person: PersonDTO) -> str:
        """
        Extracts the profile picture URL from a URL.

        Args:
            url (str): The URL.

        Returns:
            str: The profile picture URL.
        """
        if person.linkedin:
            profile_picture = self.linkedin_profile_picture_fetcher.get_profile_picture_from_linkedin_url(
                person.linkedin
            )
            if profile_picture:
                return profile_picture
        else:
            pass

    @staticmethod
    def get_profile_picture(url, platform):
        headers = {"User-Agent": "Mozilla/5.0"}

        # Ensure the URL has the scheme
        parsed_url = urlparse(url)
        if not parsed_url.scheme:
            if parsed_url.netloc:
                url = urlunparse(("https", parsed_url.netloc, parsed_url.path, "", "", ""))
            else:
                url = urlunparse(("https", parsed_url.path, "", "", "", ""))

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            match platform.lower():
                case "linkedin":
                    profile_picture = soup.find("img", {"class": "profile-photo"})
                case "facebook":
                    profile_picture = soup.find("img", {"class": "profilePic"})
                case "twitter":
                    profile_picture = soup.find("img", {"class": "ProfileAvatar-image"})
                case _:
                    return None
            if profile_picture:
                return profile_picture["src"]

        return None


def get_profile_picture(person: PersonDTO, social_links: list[SocialMediaLinks]) -> str:
    """
    Extracts the profile picture URL from a URL.

    Args:
        person (PersonDTO): The person object. Will mostly be used for the LinkedIn URL,
         but may be used for other in the future.
        social_links (list[dict]): A list of social media links.
    """
    profile_picture_fetcher = ProfilePictureFetcher()
    linkedin_picture_fetcher = LinkedInProfilePictureFetcher()

    if person and person.linkedin:
        logger.info(f"Fetching profile picture for {person.uuid} with LinkedIn URL: {person.linkedin}")
        profile_picture = (
            profile_picture_fetcher.linkedin_profile_picture_fetcher.get_profile_picture_from_linkedin_url(
                person.linkedin
            )
        )
        profile_picture = profile_picture.strip() if isinstance(profile_picture, str) else None
        profile_picture = profile_picture if 'static.licdn.com' not in profile_picture else None

        # Validate LinkedIn picture URL is working
        if profile_picture and linkedin_picture_fetcher.check_url_exists(profile_picture):
            logger.info(f"Valid LinkedIn profile picture found: {profile_picture}")
            return profile_picture
        else:
            logger.warning(f"LinkedIn profile picture URL is invalid: {profile_picture}")

    if social_links:
        logger.info(f"Fetching profile picture for {person.uuid} with social media links: {social_links}")
        for entry in social_links:
            url = str(entry.url)
            platform = entry.platform
            logger.info(f"Fetching profile picture for {person.uuid} with {platform} URL: {url}")
            if url and platform:
                picture_url = profile_picture_fetcher.get_profile_picture(url, platform)

                # Validate social media picture URL is working
                if picture_url:
                    if linkedin_picture_fetcher.check_url_exists(picture_url):
                        logger.info(f"Valid {platform} profile picture found: {picture_url}")
                        return picture_url
                    else:
                        logger.warning(f"Invalid {platform} profile picture URL: {picture_url}")
                else:
                    logger.warning(f"No {platform} profile picture found for {person.uuid}")
    # Step 5: No valid profile picture found
    logger.warning(f"No valid profile picture found for {person.uuid}")
    return 'https://monomousumi.com/wp-content/uploads/anonymous-user-8.png'
