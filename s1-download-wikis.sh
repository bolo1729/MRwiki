#!/bin/bash
# MRwiki -- tools for analyzing Wikipedia content (especially links) using Python and MapReduce. 
# Copyright (C) 2007-2012  Lukasz Bolikowski
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


# This script downloads the most recent database dumps of selected language editions of Wikipedia.
# By default, it downloads all the editions listed below.  You can override the selection by passing
# the codes of the requested editions as command-line arguments.
#
# Note that the script will store the dumps in the current working directory.

# All editions as of 2012-07-11
LANGUAGES="en de fr nl it pl es ru ja pt zh sv vi uk ca no fi cs hu ko fa id tr ar ro sk eo da sr lt ms kk he eu bg sl vo hr war hi et az gl nn simple la el th new roa-rup sh oc ka mk tl ht pms te ta be-x-old be br ceb lv sq jv mg cy mr lb is bs yo an lmo hy fy bpy ml pnb sw bn io af gu zh-yue ne uz nds ur ku ast scn su qu diq ba tt my ga cv ia nap bat-smg map-bms wa als am kn gd bug tg zh-min-nan yi sco vec hif roa-tara arz os nah mzn ky sah mn sa pam hsb li mi ckb si co gan glk bo fo bar bcl ilo mrj se fiu-vro nds-nl tk vls ps gv rue dv nrm pag pa koi rm km kv csb udm xmf mhr fur mt zea wuu lad lij ug pi sc bh zh-classical or nov ksh ang frr so kw stq nv hak ay frp ext szl pcd gag ie haw ln xal rw pdc vep pfl krc eml crh gn ace to ce kl arc myv dsb as bjn pap tpi lbe mdf wo jbo sn kab av cbk-zam ty srn kbd lez lo ab mwl ltg na ig kg tet za kaa nso zu rmy cu tn chy chr got sm bi mo iu bm pih ik ss sd pnt cdo ee ha ti bxr ts om ks ki ve sg rn cr dz lg ak ff tum fj st tw xh ch ny ng ii cho mh aa kj ho mus kr hz"

function error {
  echo "Please install wget before running this script"
  exit 1
}

which wget > /dev/null || error

if [ $# -gt 0 ]
then
  LANGUAGES="$@"
fi

LANGUAGES=`echo $LANGUAGES | sed 's/-/_/g'`

function get {
  lg=$1
  tab=$2
  tmp=`wget -O - http://dumps.wikimedia.org/${lg}wiki/latest/${lg}wiki-latest-${tab}-rss.xml 2> /dev/null | grep 'href' | sed 's/^[^"]*"//;s/".*$//'`
  if [ X$tmp == X ]
  then
    echo "WARNING: Cannot download: " $lg $tab
  else
    wget -q $tmp
    f=`echo $tmp | sed 's%^.*/%%'`
    [ ! -f $f ] && echo "WARNING: Cannot download:" $lg $tab
  fi
}

echo Started at `date`
for lg in $LANGUAGES
do
  get ${lg} "page.sql.gz" 
  get ${lg} "redirect.sql.gz" 
  get ${lg} "langlinks.sql.gz" 

  get ${lg} "categorylinks.sql.gz" 
  get ${lg} "pagelinks.sql.gz" 
done
echo Finished at `date`

