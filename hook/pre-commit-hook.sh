#!/bin/sh

#COLOR CODES:
#tput setaf 3 = yellow -> Info
#tput setaf 1 = red -> warning/not allowed commit
#tput setaf 2 = green -> all good!/allowed commit

# checking for non staged changes
git diff --quiet
hadNoNonStagedChanges=$?
if ! [ $hadNoNonStagedChanges -eq 0 ]
then
   echo "* Stashing non-staged changes"
   git stash --keep-index -u > /dev/null
fi

# checking for compilation
(sbt test:compile)
compiles=$?

echo "* Compiles?"

if [ $compiles -eq 0 ]
then
   echo "* Yes"
else
   echo "* No"
fi

# checking for formatting
(scalafmt)
git diff --quiet
formatted=$?

echo "* Properly formatted?"

if [ $formatted -eq 0 ]
then
   echo "* Yes"
else
    echo "* No"
    echo "The following files need formatting (in stage or commited):"
    git diff --name-only
    echo ""
    echo "Please run 'scalafmt' to format the code."
    echo ""
fi

# undoing formatting
git stash --keep-index > /dev/null
git stash drop > /dev/null

# if there are not staged changes, stash them
if ! [ $hadNoNonStagedChanges -eq 0 ]
then
   echo "* Scheduling stash pop of previously stashed non-staged changes for 1 second after commit."
   sleep 1 && git stash pop --index > /dev/null & # sleep and & otherwise commit fails when this leads to a merge conflict
fi

# if the code compiles and is properly formatted, end program with no errors
if [ $compiles -eq 0 ] && [ $formatted -eq 0 ]
then
   echo "... done. Proceeding with commit."
   echo ""
   exit 0

# if the code compiles but is not formatted, end program with error
elif [ $compiles -eq 0 ]
then
   echo "... done."
   echo "CANCELLING commit due to NON-FORMATTED CODE."
   echo ""
   exit 1

# if the code doesn't compile end the program with error
else
   echo "... done."
   echo "CANCELLING commit due to COMPILE ERROR."
    echo ""
   exit 2
fi

