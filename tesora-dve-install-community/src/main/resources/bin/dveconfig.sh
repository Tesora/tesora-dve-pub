#! /bin/sh

###
# #%L
# Tesora Inc.
# Database Virtualization Engine
# %%
# Copyright (C) 2011 - 2014 Tesora Inc.
# %%
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# #L%

#set -x

SELF_DIR=$(cd $(dirname "$0"); pwd)
. $SELF_DIR/common.sh

if [ "$JAVA_VER" -lt 16 ]; then
    echo "A java VM of version 1.6 or greater is required to run this script" 1>&2
    exit 1
fi

JAVA_ARGS="-Dlog4j.configuration=dveconfiglog4j.properties -Dtesora.dve.log=$LOG_DIR"

"$JAVA_PROG" -classpath $CLASSPATH $JAVA_ARGS com.tesora.dve.tools.DVEConfigCLI "$@"

exit $?

