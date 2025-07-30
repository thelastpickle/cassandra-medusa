#!/usr/bin/env bash

# Run from packaging folder with `./release.sh`

set -xe

# Install dependency if not installed
which bump2version || pip3 install bump2version

cd ..
initial_branch=$(git rev-parse --abbrev-ref HEAD)
current_branch=$(git rev-parse --abbrev-ref HEAD)
release_branch=$(cat VERSION | cut -d"." -f1-2)


if [ "$current_branch" == "master" ]
then
    # We need to create a new release branch
    echo "Creating release branch $release_branch"
    git checkout -b $release_branch
    current_branch=$(git rev-parse --abbrev-ref HEAD)
fi

# Check that we're on the release branch
if [ "$current_branch" != "$release_branch" ]
then
    echo "You are not releasing from the right branch. Please create the release from branch '$release_branch'"
    exit 1
fi

# Bump version for release
if [ "$(cat VERSION | cut -d"-" -f1)" != "$(cat VERSION | cut -d"-" -f2)" ]
then
    echo "Bumping version number for release"
    bump2version --no-tag release --message "Release {new_version}"
fi

RELEASE_VERSION=$(cat VERSION)

echo "Please check that the changelog is up to date before tagging the release:"
awk '/^###/ { f = 1; n++ } f && n == wanted; /^$/ { f = 0 }' wanted=1 CHANGELOG.md
changelog_ok=unknown
while [ "$changelog_ok" != "y" ] && [ "$changelog_ok" != "n" ]
do
    read -p 'Is the changelog up to date? [y/n]' changelog_ok
done

changelog_updated="n"
if [ "$changelog_ok" == "n" ]
then
    hash_before=$(md5 CHANGELOG.md)
    vi CHANGELOG.md
    hash_after=$(md5 CHANGELOG.md)
    if [ "$hash_before" == "$hash_after" ]
    then
        echo "Changelog was not updated. Exiting..."
        exit 1
    fi
    changelog_updated="y"
fi

version_exists=$(cat debian/changelog | grep "($RELEASE_VERSION)" || echo "")
if [ -z "$version_exists" ]
then
    echo "Updating debian changelog..."
    cd packaging/docker-build
    docker-compose build release && docker-compose run release
    changelog_updated="y"
    cd ../..
fi

if [ "$changelog_updated" == "y" ]
then
    git commit -a -m "Update changelog for release $RELEASE_VERSION"
fi

git tag -a v$RELEASE_VERSION -m "Release v$RELEASE_VERSION"
git push -u origin $release_branch --follow-tags
git push origin v$RELEASE_VERSION

# post release work

if [ "$initial_branch" == "master" ]
then
    # if we released a new major/minor from master
    git checkout master
    bump2version minor
else
    # if we released a new patch version from a release branch
    bump2version patch
fi

echo "Release done successfully!"
echo "You now need to publish the draft release on GitHub: https://github.com/thelastpickle/cassandra-medusa/releases"
echo "Once the release build goes through in GitHub Actions, please perform a forward merge from the release branch as described here: http://cassandra-reaper.io/docs/development/ "
