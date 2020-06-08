#!/bin/bash
#
# Author: Michael Buchel
# Company: MIM Technology Group Inc.
# Reason: Because my company servers do not allow for python script to be executed,
# NOTE: Requires ssh access to server you host it on.
#
cd $1/coronavirus_map
./bin/manual_override
scp *.json $2:public_html/coronavirus/
rm *.json
