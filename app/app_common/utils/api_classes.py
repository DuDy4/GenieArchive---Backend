import re
import requests
from urllib.parse import urlparse, unquote

from loguru import logger


class GoogleSearchAPI:
    def __init__(self, key: str, cx: str):
        self._cx = cx
        self._key = key
        self._api_url = "https://www.googleapis.com/customsearch/v1"
        self._params = {"num": 10, "cx": self._cx, "key": self._key}

    def _hit_api(self, linkedin_id: str) -> object:
        api_response = APIResponse()
        try:
            params = self._params
            params["exactTerms"] = f"/in/{linkedin_id}"
            resp = requests.get(self._api_url, params=params)
            api_response = self._create_api_response(linkedin_id, resp)
        except Exception as e:
            logger.info(f"Error in _hit_api: {e}", exc_info=True)
        return api_response

    def _create_api_response(self, linkedin_id: str, resp: object) -> object:
        link = ""
        results = []
        error = None
        status_code = resp.status_code
        if status_code == 200:
            results = resp.json()
            results = results.get("items", [])
        else:
            error = resp.json()
        return APIResponse(results, linkedin_id, status_code, link, error)


class APIResponse:
    def __init__(
        self, _search_results=[], linkedin_id="", status_code=400, link="", error=None
    ):
        self._search_results = _search_results
        self.linkedin_id = linkedin_id
        self.status_code = status_code
        self.link = link
        self.error = error


class ProfilePicture(object):
    def __init__(self, key: str, cx: str):
        self._api_obj = GoogleSearchAPI(key, cx)

    def extract_id(self, link: str) -> str:
        """
        To get clean linkedin id
        Example:
            Input  : linkedin.com/in/shashank-deshpande/
            Output : shashank-deshpande
        """
        linkedin_id = link
        match = re.findall(r"\/in\/([^\/]+)\/?", urlparse(link).path)
        if match:
            linkedin_id = match[0].strip()
        linkedin_id = linkedin_id.strip("/")
        linkedin_id = unquote(linkedin_id)
        return linkedin_id

    def _check_picture_url(self, link: str) -> bool:
        match = re.findall(
            r"(media-exp\d\.licdn\.com).+?(profile-displayphoto-shrink_)", link
        )
        return bool(match)

    def _check_url_exists(self, link):
        flag = False
        try:
            resp = requests.get(link, timeout=5)
            if resp and resp.status_code == 200:
                flag = True
        except:
            pass
        return flag

    def _extract_profile_picture(self, linkedin_id: str, res: list) -> str:
        link = ""
        for i in res:
            linkedin_url = i.get("link", "")
            search_id = self.extract_id(linkedin_url)
            if search_id == linkedin_id:
                metatags = i.get("pagemap", {}).get("metatags", [])
                metatags = sum(
                    list(
                        map(
                            lambda mt: list(
                                dict(
                                    filter(lambda x: "image" in x[1], mt.items())
                                ).values()
                            ),
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
                    if self._check_picture_url(url) and self._check_url_exists(url):
                        link = url
                        break
            if link:
                break
        return link

    def search(self, link: str) -> object:
        linkedin_id = self.extract_id(link)
        api_resp = self._api_obj._hit_api(linkedin_id)
        api_resp.link = self._extract_profile_picture(
            linkedin_id, api_resp._search_results
        )
        api_resp.linkedin_id = linkedin_id
        return api_resp
