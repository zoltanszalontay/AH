rem @echo off

set /p message="Commit message: "

if not [%1] == [] goto :branch
set localbranch=zoltansz
goto :git

echo This should not be executed

:branch
set localbranch=%1

:git
git.exe checkout master --
git.exe pull --progress -v --no-rebase "origin"
git.exe checkout %localbranch% --
git.exe commit -am "%message%"
git.exe merge master
git.exe push --progress "origin" %localbranch%:%localbranch%
git.exe checkout master --
git.exe merge %localbranch%
git.exe push --progress "origin" master:master