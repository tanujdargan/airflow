/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Box, Flex, Heading, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiClipboard, FiZap } from "react-icons/fi";

import { useAuthLinksServiceGetAuthMenus, useDashboardServiceDagStats } from "openapi/queries";
import { useAutoRefresh } from "src/utils";

import { DAGImportErrors } from "./DAGImportErrors";
import { PluginImportErrors } from "./PluginImportErrors";
import { StatsCard } from "./StatsCard";

export const Stats = () => {
  const refetchInterval = useAutoRefresh({});
  const { data: statsData, isLoading: isStatsLoading } = useDashboardServiceDagStats(undefined, {
    refetchInterval,
  });
  const { data: authLinks } = useAuthLinksServiceGetAuthMenus();
  const failedDagsCount = statsData?.failed_dag_count ?? 0;
  const queuedDagsCount = statsData?.queued_dag_count ?? 0;
  const runningDagsCount = statsData?.running_dag_count ?? 0;
  const activeDagsCount = statsData?.active_dag_count ?? 0;
  const { t: translate } = useTranslation("dashboard");

  return (
    <Box>
      <Flex alignItems="center" color="fg.muted" my={2}>
        <FiClipboard />
        <Heading ml={1} size="xs">
          {translate("stats.stats")}
        </Heading>
      </Flex>

      <HStack gap={4}>
        <StatsCard
          colorScheme="failed"
          count={failedDagsCount}
          isLoading={isStatsLoading}
          label={translate("stats.failedDags")}
          link="dags?last_dag_run_state=failed"
          state="failed"
        />

        <DAGImportErrors />

        {authLinks?.authorized_menu_items.includes("Plugins") ? <PluginImportErrors /> : null}

        {queuedDagsCount > 0 ? (
          <StatsCard
            colorScheme="queued"
            count={queuedDagsCount}
            isLoading={isStatsLoading}
            label={translate("stats.queuedDags")}
            link="dags?last_dag_run_state=queued"
            state="queued"
          />
        ) : undefined}

        <StatsCard
          colorScheme="running"
          count={runningDagsCount}
          isLoading={isStatsLoading}
          label={translate("stats.runningDags")}
          link="dags?last_dag_run_state=running"
          state="running"
        />

        <StatsCard
          colorScheme="blue"
          count={activeDagsCount}
          icon={<FiZap />}
          isLoading={isStatsLoading}
          label={translate("stats.activeDags")}
          link="dags?paused=false"
        />
      </HStack>
    </Box>
  );
};
