# pushing to origin repository your master branch 
sudo git push origin master

# pulling from origin repository the master branch 
sudo git pull origin master


# git merge conflict
-- install diffmerge
sudo git config --global merge.tool diffmerge
sudo git config --global mergetool.diffmerge.trustExitCode true

#Force Git to overwrite local files on pull
sudo git fetch --all
sudo git reset --hard origin/master