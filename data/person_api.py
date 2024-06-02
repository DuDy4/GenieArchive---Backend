v1_router = APIRouter(prefix="/v1")
app = FastAPI()
app.include_router(v1_router)


@v1_router.get("/profile/{uuid}")
async def get_profile(
    request: Request,
    uuid: str,
    person_repository: PersonsRepository = Depends(persons_repository),
):
    try:
        profile = person_repository.get_person_by_uuid(uuid)
        return profile
    except Exception as e:
        logger.error(f"Failed to get profile: {e}")
