from git import Repo
import os

join = os.path.join

# rorepo is a Repo instance pointing to the git-python repository.
# For all you know, the first argument to Repo is a path to the repository
# you want to work with
repo = Repo()
assert not repo.bare