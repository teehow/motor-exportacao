#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from migracao_contrato_mapping import apply_contrato_mapping
from migracao_project_filter import parse_project_args
import logging
import os
from pathlib import Path
from time import perf_counter

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("export_colaboradores")

# 02.COLABORADORES (company_id parametrizado)
PROJECT_FILTER_CTES = r"""
projetos_filtrados as (
    select PRJ.id, PRJ.company_id, PRJ.name, PRJ.deleted, PRJ.start_date, PRJ.end_date
    from project as PRJ
    where PRJ.company_id = %(company_id)s
      and PRJ.deleted = false
      and (%(project_names)s::text[] is null or PRJ.name = any(%(project_names)s::text[]))
      and (%(project_ids)s::integer[] is null or PRJ.id = any(%(project_ids)s::integer[]))
),
contratadas_filtradas as (
    select PCF.id, PCF.project_id, PCF.company_id
    from projetos_filtrados as PF
    inner join project_company as PCF on PCF.project_id = PF.id and PCF.deleted = false
    inner join company as CF on CF.id = PCF.company_id and CF.deleted = false
    where %(contratada_cnpjs)s::text[] is not null
      and regexp_replace(CF.cnpj, '[^0-9]', '', 'g') = any(%(contratada_cnpjs)s::text[])
)
"""

SQL_SELECT = "with " + PROJECT_FILTER_CTES + r"""

, pendencias_filtradas as (
    select pendency.id, pendency.employee_id, pendency.project_id, pendency.pendency_id,
           pendency.deleted, pendency.active_status, pendency.plan_id, pendency.reference_month, pendency.status
    from pendency
    inner join projetos_filtrados as PF on PF.id = pendency.project_id
    where (%(pendency_ids)s::integer[] is null
       or pendency.pendency_id = any(%(pendency_ids)s::integer[]))
      and (pendency.deleted = false or (
          pendency.deleted = true and (
              %(considerar_inactive)s and (
                  pendency.plan_id = 1 and pendency.active_status in (
                      'INACTIVE_BY_MOVEMENT', 'INACTIVE_BY_PROJECT_DEACTIVATION',
                      'INACTIVE_BY_PROJECT_COMPANY_INACTIVATION'
                  )
              )
          )
      ))
),
alvos_pendencias as (
    select PCY.project_id, PCY.employee_id
    from pendencias_filtradas as PCY
    where %(pendency_ids)s::integer[] is not null and PCY.employee_id is not null

    union

    select PCY.project_id, PE.employee_id
    from pendencias_filtradas as PCY
    inner join pendency_employee as PE on PE.pendency_id = PCY.id and PE.deleted = false
    where %(pendency_ids)s::integer[] is not null and PE.employee_id is not null
),
-- Materializa os IDs do bloco antes de consultar employee, evitando EXISTS por funcionario.
contratos_colaboradores_bloco as materialized (
    select CTF.employee_id, CTF.project_id
    from projetos_filtrados as PF
    inner join contract as CTF on CTF.project_id = PF.id and CTF.deleted = false
    where %(pendency_ids)s::integer[] is null and CTF.employee_id is not null

    union

    select CTF.employee_id, CTF.project_id
    from alvos_pendencias as AP
    inner join contract as CTF on CTF.project_id = AP.project_id
        and CTF.employee_id = AP.employee_id and CTF.deleted = false
    where %(pendency_ids)s::integer[] is not null
),
colaboradores_elegiveis as materialized (
    select distinct EMP.id as employee_id
    from contratos_colaboradores_bloco as CTF
    inner join employee as EMP on EMP.id = CTF.employee_id
        and EMP.deleted = false and EMP.name is not null
    where exists (
        select 1 from project_company as PCF
        where PCF.project_id = CTF.project_id and PCF.company_id = EMP.company_id
          and PCF.deleted = false
    )
      and (%(contratada_cnpjs)s::text[] is null or exists (
          select 1 from contratadas_filtradas as CF
          where CF.project_id = CTF.project_id and CF.company_id = EMP.company_id
      ))
),
-- Indicador reutilizado como conjunto de chaves, sem buscas por linha exportada.
pendencias_abertas as materialized (
    select PCY.project_id, PCY.employee_id, PCY.pendency_id
    from pendencias_filtradas as PCY
    where PCY.employee_id is not null
      and PCY.status in ('OPEN', 'IN_REVISION', 'EXPIRED')
      and (PCY.deleted = false or exists (
          select 1 from pendency_employee as PE
          where PE.pendency_id = PCY.id and PE.deleted = false
      ))
),
colaboradores_filtrados as (
    select EMP.id, EMP.company_id, EMP.deleted, EMP.name, EMP.document, EMP.pis,
           EMP.gender_id, EMP.foreign, EMP.birthdate,
           (select DOC.document
            from employee_document as DOC
            inner join document_type as TYP on TYP.id = DOC.doc_type_id
                and TYP.name = 'RG' and TYP.deleted = false
            where DOC.deleted = false and DOC.employee_id = EMP.id
            limit 1) as colaborador_rg,
           (select DOC.document
            from employee_document as DOC
            inner join document_type as TYP on TYP.id = DOC.doc_type_id
                and TYP.name = 'Passaporte' and TYP.deleted = false
            where DOC.deleted = false and DOC.employee_id = EMP.id
            limit 1) as colaborador_passaporte
    from colaboradores_elegiveis as CE
    inner join employee as EMP on EMP.id = CE.employee_id
),
demissao_candidatos as not materialized (
select
        PRJ.id   			as project_id
	  , CPN.id				as contratada_identificador
	  , EMP.id				as colaborador_identificador
	  , EAT.activity_type_id 		 as colaborador_id_motivo_demissao
      , EAT.movimentation_start_date as colaborador_data_desligamento
      , 0 							 as competencia_tipo
      , PCY.id 						 as pendencia_id
      , ROW_NUMBER() OVER (
              PARTITION BY
                  PRJ.id
                , CPN.id
                , EMP.id
                , DATE_TRUNC('month', EAT.movimentation_start_date)
              ORDER BY EAT.id DESC
          ) AS rn
from   employee as EMP
        inner join contract as CTT
                on CTT.employee_id = EMP.id
               and CTT.project_id is not null
               and CTT.deleted = false
		inner join employee_activity_type as EAT
		        on EAT.employee_id = EMP.id
		       and EAT.deleted = false
               and EAT.activity_type_id not in (1, 35)
        inner join project_company as PRJ_CPN
                on PRJ_CPN.company_id = EMP.company_id
               and PRJ_CPN.project_id is not null
               and PRJ_CPN.deleted = false
        inner join projetos_filtrados as PRJ
                on PRJ.id = PRJ_CPN.project_id
               and PRJ.id = CTT.project_id
               and PRJ.deleted = false
        inner join company as CPN
                on CPN.id = EMP.company_id
        inner join pendency as PCY
                on PCY.employee_id = EMP.id
               and PCY.project_id = PRJ.id
               and PCY.deleted = false
        inner join system_pendency as SYP
                on SYP.id = PCY.pendency_id
               and SYP.deleted = false
               and SYP.internal = false               
        inner join system_pendency_activity_type as SPAT 
                on SPAT.system_pendency_id = PCY.pendency_id 
               and SPAT.activity_type_id  = EAT.activity_type_id 
               and SPAT.deleted = false
where   PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      where CF.project_id = PRJ.id and CF.company_id = CPN.id
  ))
  and (%(project_ids)s::integer[] is null or PRJ.id = any(%(project_ids)s::integer[]))
  and (%(pendency_ids)s::integer[] is null or PCY.pendency_id = any(%(pendency_ids)s::integer[]))
  and   EMP.name is not null
  and   EMP.deleted = false
  and   PCY.plan_id = 2 
  and   EAT.movimentation_start_date >= date_trunc('month', PCY.reference_month)
  and   EAT.movimentation_start_date < date_trunc('month', PCY.reference_month) + interval '1 month'),
-- Mantem o ranking mensal, calculando apenas uma vez os registros selecionados.
colaborador_demissao as materialized (
    select *
    from demissao_candidatos
    where rn = 1
)

select
        CTR.cnpj		as contratante_cnpj
	  , PRJ.name as contrato_numero
	  , PRJ.id as _contrato_project_id
	  , CPN.id			as contratada_identificador
	  , case 
         when (case
               when coalesce(PCY.plan_id, PLN.id) = 2 then coalesce(PCY.reference_month, date_trunc('month', PRJ.start_date))
               else CTT.created_at 
               end) < PRJ.start_date then PRJ.start_date
         when (case
               when coalesce(PCY.plan_id, PLN.id) = 2 then coalesce(PCY.reference_month, date_trunc('month', PRJ.start_date))
               else CTT.created_at 
               end) > PRJ.end_date then PRJ.end_date
         else (case
               when coalesce(PCY.plan_id, PLN.id) = 2 then coalesce(PCY.reference_month, date_trunc('month', PRJ.start_date))
               else CTT.created_at 
               end)
         end                        as competencia_data
	 , case
	     when coalesce(PCY.plan_id, PLN.id) = 2 then 0
		 else 1 
		 end						as competencia_tipo
	  , EMP.id				as colaborador_identificador
	  , CTT.registration	as colaborador_matricula
	  , EMP.document		as colaborador_cpf
	  , EMP.colaborador_rg
	  , EMP.colaborador_passaporte
	  , EMP.pis				as colaborador_pis
	  , EMP.name			as colaborador_nome
	  , case EMP.gender_id
           when 2 then 'F'
           when 4 then 'M'
           else null
        end					as colaborador_sexo
	  , case when EMP.foreign = true then 1
	         else 0 end		as colaborador_estrangeiro
	  , EMP.birthdate		as colaborador_data_nascimento
	  , NULL				as colaborador_email
	  , NULL				as colaborador_observacao
	  , coalesce(case when CTT.role_id = 99999 then CTT.additional_role
	                  else RLO.name end, 'A informar')	as colaborador_funcao                     
	  , coalesce(CTT.base_salary, 0)    as colaborador_salario                    
	  , NULL				as colaborador_local_servicos             
	  , case CTT.exposure_type
           when 'DANGEROUSNESS' then 'PERICULOSIDADE'
           when 'UNHEALTHY' then 'INSALUBRIDADE'
           else 'NÃO'
        end					as colaborador_exposicao_risco            
	  , CTT.start_date		as colaborador_data_inicio_servico        
	  , CTT.end_date		as colaborador_data_termino_contrato      
	  , 1					as colaborador_id_motivo_admissao         
	  , CTT.hiring_date		as colaborador_data_admissao              
	  , case CLD.colaborador_id_motivo_demissao
            when 2  then 1 
            when 3  then 2
            when 4  then 3
            when 5  then 4
            when 6  then 5
            when 7  then 6
            when 8  then 7
            when 9  then 8
            when 10 then 9
            when 11 then 30
            when 12 then 34
            when 13 then 11
            when 14 then 10
            when 15 then 11
            when 16 then 12
            when 17 then 13
            when 18 then 14
            when 19 then 16
            when 20 then 17
            when 21 then 19
            when 22 then 20
            when 23 then 21
            when 24 then 25
            when 25 then 31
            when 26 then 33
            when 27 then 22
            when 28 then 23
            when 29 then 24
            when 30 then 32
            when 31 then 26
            when 32 then 27
            when 33 then 28
            when 34 then 29     
        end                             		    as colaborador_id_motivo_demissao         
	  , CLD.colaborador_data_desligamento   		as colaborador_data_desligamento          
	  , case when exists (select 1
                  from   employee_activity_type as EAT
                  where  EAT.employee_id = EMP.id
                  and    EAT.deleted = false
                  and    EAT.activity_type_id = 35
                  and    EAT.movimentation_start_date >= date_trunc('month', PCY.reference_month)
                  and    EAT.movimentation_start_date < date_trunc('month', PCY.reference_month) + interval '1 month'
                  ) then 1 else 0 end as colaborador_ferias
	  , case CTT_RGM.id
            when 1 then 2
            when 2 then 4
            when 3 then 5
            when 4 then 6
            when 5 then 7
            when 6 then 8
            when 7 then 9
            when 8 then 9
            when 9 then 10
            when 10 then 13
            when 11 then 12
            when 12 then 11
            when 13 then 16
            when 14 then 5
        end                 as colaborador_id_classificacao           
	  , NULL				as colaborador_refeicao_custeada          
	  , 1					as colaborador_tipo_prestador             
	  , 0					as colaborador_amostragem                 
	  , case when (PRJ.id, EMP.id) in (
            select PAB.project_id, PAB.employee_id
            from pendencias_abertas as PAB
            inner join system_pendency as SYP on SYP.id = PAB.pendency_id
                and SYP.deleted = false and SYP.internal = false
        ) then 1 else 0 end as colaborador_possui_pendencia       
from    colaboradores_filtrados as EMP
        inner join contract as CTT
                on CTT.employee_id = EMP.id
               and CTT.project_id is not null
               and CTT.deleted = false
        inner join project_company as PRJ_CPN
                on PRJ_CPN.company_id = EMP.company_id
               and PRJ_CPN.project_id is not null
               and PRJ_CPN.deleted = false
        inner join project_company_plan as PCP
                on PCP.project_company_id = PRJ_CPN.id
               and PCP.deleted = false
        inner join plan as PLN
                on PLN.id  = PCP.plan_id
               and PLN.deleted = false
        inner join projetos_filtrados as PRJ
                on PRJ.id = PRJ_CPN.project_id
               and PRJ.id = CTT.project_id
               and PRJ.deleted = false
        inner join company as CPN
                on CPN.id = EMP.company_id
        left  join role as RLO
                on RLO.id = CTT.role_id
		inner join company as CTR
				on CTR.id = PRJ.company_id
		inner join contract_regime as CTT_RGM
				on CTT_RGM.id = CTT.contract_regime_id
			   and CTT_RGM.deleted = false
        left join lateral (
            select P.*
            from pendency as P
            where P.employee_id = EMP.id
              and (%(pendency_ids)s::integer[] is null or P.pendency_id = any(%(pendency_ids)s::integer[]))
              and (P.deleted = false or (
                  P.deleted = true and (
                      %(considerar_inactive)s and (
                          P.plan_id = 1 and P.active_status in (
                              'INACTIVE_BY_MOVEMENT', 'INACTIVE_BY_PROJECT_DEACTIVATION',
                              'INACTIVE_BY_PROJECT_COMPANY_INACTIVATION'
                          )
                      )
                  )
              ))
            offset 0
        ) as PCY on PCY.project_id = PRJ.id
        left join colaborador_demissao as CLD
                on CLD.project_id = PRJ.id
               and CLD.contratada_identificador = CPN.id
               and CLD.colaborador_identificador = EMP.id
               and CLD.pendencia_id = PCY.id
where   PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      where CF.project_id = PRJ.id and CF.company_id = CPN.id
  ))
  and (%(project_ids)s::integer[] is null or PRJ.id = any(%(project_ids)s::integer[]))
  and (%(pendency_ids)s::integer[] is null or PCY.pendency_id = any(%(pendency_ids)s::integer[]))
  and   EMP.name is not null
  and   EMP.deleted = false
union
select
        CTR.cnpj		as contratante_cnpj
	  , PRJ.name as contrato_numero
	  , PRJ.id as _contrato_project_id
	  , CPN.id			as contratada_identificador
	  , case 
         when (case
               when coalesce(PCY.plan_id, PLN.id) = 2 then coalesce(PCY.reference_month, date_trunc('month', PRJ.start_date))
               else CTT.created_at 
               end) < PRJ.start_date then PRJ.start_date
         when (case
               when coalesce(PCY.plan_id, PLN.id) = 2 then coalesce(PCY.reference_month, date_trunc('month', PRJ.start_date))
               else CTT.created_at 
               end) > PRJ.end_date then PRJ.end_date
         else (case
               when coalesce(PCY.plan_id, PLN.id) = 2 then coalesce(PCY.reference_month, date_trunc('month', PRJ.start_date))
               else CTT.created_at 
               end)
         end                        as competencia_data
	 , case
	     when coalesce(PCY.plan_id, PLN.id) = 2 then 0
		 else 1 
		 end						as competencia_tipo
	  , EMP.id				as colaborador_identificador
	  , CTT.registration	as colaborador_matricula
	  , EMP.document		as colaborador_cpf
	  , EMP.colaborador_rg
	  , EMP.colaborador_passaporte
	  , EMP.pis				as colaborador_pis
	  , EMP.name			as colaborador_nome
	  , case EMP.gender_id
           when 2 then 'F'
           when 4 then 'M'
           else null
        end					as colaborador_sexo
	  , case when EMP.foreign = true then 1
	         else 0 end		as colaborador_estrangeiro
	  , EMP.birthdate		as colaborador_data_nascimento
	  , NULL				as colaborador_email
	  , NULL				as colaborador_observacao
	  , coalesce(case when CTT.role_id = 99999 then CTT.additional_role
	                  else RLO.name end, 'A informar')	as colaborador_funcao                     
	  , coalesce(CTT.base_salary, 0)    as colaborador_salario                    
	  , NULL				as colaborador_local_servicos             
	  , case CTT.exposure_type
           when 'DANGEROUSNESS' then 'PERICULOSIDADE'
           when 'UNHEALTHY' then 'INSALUBRIDADE'
           else 'NÃO'
        end					as colaborador_exposicao_risco            
	  , CTT.start_date		as colaborador_data_inicio_servico        
	  , CTT.end_date		as colaborador_data_termino_contrato      
	  , 1					as colaborador_id_motivo_admissao         
	  , CTT.hiring_date		as colaborador_data_admissao              
	  , case CLD.colaborador_id_motivo_demissao
            when 2  then 1 
            when 3  then 2
            when 4  then 3
            when 5  then 4
            when 6  then 5
            when 7  then 6
            when 8  then 7
            when 9  then 8
            when 10 then 9
            when 11 then 30
            when 12 then 34
            when 13 then 11
            when 14 then 10
            when 15 then 11
            when 16 then 12
            when 17 then 13
            when 18 then 14
            when 19 then 16
            when 20 then 17
            when 21 then 19
            when 22 then 20
            when 23 then 21
            when 24 then 25
            when 25 then 31
            when 26 then 33
            when 27 then 22
            when 28 then 23
            when 29 then 24
            when 30 then 32
            when 31 then 26
            when 32 then 27
            when 33 then 28
            when 34 then 29     
        end                                         as colaborador_id_motivo_demissao         
	  , CLD.colaborador_data_desligamento   		as colaborador_data_desligamento          
	  , case when exists (select 1
                  from   employee_activity_type as EAT
                  where  EAT.employee_id = EMP.id
                  and    EAT.deleted = false
                  and    EAT.activity_type_id = 35
                  and    EAT.movimentation_start_date >= date_trunc('month', PCY.reference_month)
                  and    EAT.movimentation_start_date < date_trunc('month', PCY.reference_month) + interval '1 month'
                  ) then 1 else 0 end as colaborador_ferias
	  , case CTT_RGM.id
            when 1 then 2
            when 2 then 4
            when 3 then 5
            when 4 then 6
            when 5 then 7
            when 6 then 8
            when 7 then 9
            when 8 then 9
            when 9 then 10
            when 10 then 13
            when 11 then 12
            when 12 then 11
            when 13 then 16
            when 14 then 5
        end                 as colaborador_id_classificacao           
	  , NULL				as colaborador_refeicao_custeada          
	  , 1					as colaborador_tipo_prestador             
	  , 0					as colaborador_amostragem                 
	  , case when (PRJ.id, EMP.id) in (
            select PAB.project_id, PAB.employee_id
            from pendencias_abertas as PAB
        ) then 1 else 0 end as colaborador_possui_pendencia       
from    colaboradores_filtrados as EMP
        inner join contract as CTT
                on CTT.employee_id = EMP.id
               and CTT.project_id is not null
               and CTT.deleted = false
        inner join project_company as PRJ_CPN
                on PRJ_CPN.company_id = EMP.company_id
               and PRJ_CPN.project_id = CTT.project_id
               and PRJ_CPN.deleted = false
        inner join project_company_plan as PCP
                on PCP.project_company_id = PRJ_CPN.id
               and PCP.deleted = false
        inner join plan as PLN
                on PLN.id  = PCP.plan_id
               and PLN.deleted = false
        inner join projetos_filtrados as PRJ
                on PRJ.id = PRJ_CPN.project_id
               and PRJ.id = CTT.project_id
               and PRJ.deleted = false
        inner join company as CPN
                on CPN.id = EMP.company_id
        left  join role as RLO
                on RLO.id = CTT.role_id
		inner join company as CTR
				on CTR.id = PRJ.company_id
		inner join contract_regime as CTT_RGM
				on CTT_RGM.id = CTT.contract_regime_id
			   and CTT_RGM.deleted = false
        inner join pendency_employee as PCY_EMP
                on PCY_EMP.employee_id = EMP.id
               and PCY_EMP.deleted = false
        inner join pendency as PCY
                on PCY.id = PCY_EMP.pendency_id
               and (%(pendency_ids)s::integer[] is null or PCY.pendency_id = any(%(pendency_ids)s::integer[]))
               and PCY.project_id = PRJ.id
               and (PCY.deleted = false 
                    or (PCY.deleted = true
                        and (
                            %(considerar_inactive)s and (
                                PCY.plan_id = 1 and PCY.active_status in (
                                    'INACTIVE_BY_MOVEMENT', 'INACTIVE_BY_PROJECT_DEACTIVATION',
                                    'INACTIVE_BY_PROJECT_COMPANY_INACTIVATION'
                                )
                            )
                        )
                    ))
        inner join system_pendency as SYP
                on SYP.id = PCY.pendency_id
               and SYP.deleted = false
               and SYP.internal = false               
        left join colaborador_demissao as CLD
                on CLD.project_id = PRJ.id
               and CLD.contratada_identificador = CPN.id
               and CLD.colaborador_identificador = EMP.id
               and CLD.pendencia_id = PCY.id
where   PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF
      where CF.project_id = PRJ.id and CF.company_id = CPN.id
  ))
  and (%(project_ids)s::integer[] is null or PRJ.id = any(%(project_ids)s::integer[]))
  and (%(pendency_ids)s::integer[] is null or PCY.pendency_id = any(%(pendency_ids)s::integer[]))
  and   EMP.name is not null
  and   EMP.deleted = false
"""


PROJECT_IDS_SQL = "with " + PROJECT_FILTER_CTES + r"""
select PRJ.id
from projetos_filtrados as PRJ
where PRJ.company_id = %(company_id)s
  and (%(contratada_cnpjs)s::text[] is null or exists (
      select 1 from contratadas_filtradas as CF where CF.project_id = PRJ.id
  ))
  and PRJ.deleted = false
  and (
      %(pendency_ids)s::integer[] is null
      or exists (
          select 1 from pendency as PCY
          where PCY.project_id = PRJ.id
            and PCY.pendency_id = any(%(pendency_ids)s::integer[])
      )
  )
order by PRJ.id
"""


PARQUET_SCHEMA = pa.schema([
    ("contratante_cnpj", pa.large_string()),
    ("contrato_numero", pa.large_string()),
    ("contratada_identificador", pa.int64()),
    ("competencia_data", pa.timestamp("us")),
    ("competencia_tipo", pa.int64()),
    ("colaborador_identificador", pa.int64()),
    ("colaborador_matricula", pa.large_string()),
    ("colaborador_cpf", pa.large_string()),
    ("colaborador_rg", pa.large_string()),
    ("colaborador_passaporte", pa.large_string()),
    ("colaborador_pis", pa.large_string()),
    ("colaborador_nome", pa.large_string()),
    ("colaborador_sexo", pa.large_string()),
    ("colaborador_estrangeiro", pa.int64()),
    ("colaborador_data_nascimento", pa.timestamp("us")),
    ("colaborador_email", pa.large_string()),
    ("colaborador_observacao", pa.large_string()),
    ("colaborador_funcao", pa.large_string()),
    ("colaborador_salario", pa.float64()),
    ("colaborador_local_servicos", pa.large_string()),
    ("colaborador_exposicao_risco", pa.large_string()),
    ("colaborador_data_inicio_servico", pa.timestamp("us")),
    ("colaborador_data_termino_contrato", pa.timestamp("us")),
    ("colaborador_id_motivo_admissao", pa.int64()),
    ("colaborador_data_admissao", pa.timestamp("us")),
    ("colaborador_id_motivo_demissao", pa.int64()),
    ("colaborador_data_desligamento", pa.timestamp("us")),
    ("colaborador_ferias", pa.int64()),
    ("colaborador_id_classificacao", pa.int64()),
    ("colaborador_refeicao_custeada", pa.int64()),
    ("colaborador_tipo_prestador", pa.int64()),
    ("colaborador_amostragem", pa.int64()),
    ("colaborador_possui_pendencia", pa.int64()),
])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta COLABORADORES para Parquet.")
    p.add_argument("--company-id", type=int, required=True)
    p.add_argument("--out", required=True, help="Arquivo parquet de saída")

    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--db", dest="dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", default=None, help="Opcional se usar .pgpass")
    p.add_argument("--sslmode", default="require")

    p.add_argument("--chunksize", type=int, default=50_000, help="Linhas por chunk (padrão: 50000)")
    p.add_argument("--compression", default="snappy", help="snappy|gzip|zstd|none")

    p.add_argument(
        "--project-block-size", type=int,
        default=int(os.getenv("MIGRACAO_COLABORADORES_PROJECT_BLOCK_SIZE", "50")),
        help="Projetos por bloco de consulta. Use 0 para consultar a empresa inteira.",
    )
    return parse_project_args(p)


def ensure_output_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    if path.is_dir():
        raise ValueError(f"--out precisa ser um arquivo (não um diretório): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def build_project_blocks(conn, args: argparse.Namespace) -> list[list[int] | None]:
    if args.project_block_size <= 0:
        return [None]
    with conn.cursor() as cur:
        cur.execute(PROJECT_IDS_SQL, {
            "company_id": args.company_id,
            "project_names": args.project_names,
            "project_ids": None,
            "pendency_ids": args.pendency_ids,
            "contratada_cnpjs": args.contratada_cnpjs,
            "considerar_inactive": args.considerar_inactive,
        })
        project_ids = [row[0] for row in cur.fetchall()]
    return [project_ids[index:index + args.project_block_size]
            for index in range(0, len(project_ids), args.project_block_size)]


def export_to_parquet(args: argparse.Namespace, out_path: Path) -> None:
    if args.chunksize <= 0:
        raise ValueError("--chunksize precisa ser maior que zero.")
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        sslmode=args.sslmode,
    )

    parquet_writer = None
    total = 0
    timings = dict(leitura=0.0, dataframe=0.0, arrow=0.0, escrita=0.0)

    try:
        logger.info("Executando consulta (company_id=%s)...", args.company_id)

        project_blocks = build_project_blocks(conn, args)
        compression = None if args.compression.lower() == "none" else args.compression.lower()
        parquet_writer = pq.ParquetWriter(out_path, PARQUET_SCHEMA, compression=compression)
        logger.info("Blocos de projetos: %s", len(project_blocks))
        for block_index, project_ids in enumerate(project_blocks, start=1):
            block_started = perf_counter()
            block_read_started = timings["leitura"]
            logger.info("Bloco %s/%s: projetos=%s", block_index, len(project_blocks), project_ids)
            block_rows = 0
            cursor_name = f"export_colaboradores_{os.getpid()}_{block_index}"
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = args.chunksize
                cur.execute(SQL_SELECT, {
                    "company_id": args.company_id,
                    "project_names": args.project_names,
                    "pendency_ids": args.pendency_ids,
                    "contratada_cnpjs": args.contratada_cnpjs,
                    "project_ids": project_ids,
                    "considerar_inactive": args.considerar_inactive,
                })
                while True:
                    started = perf_counter()
                    rows = cur.fetchmany(args.chunksize)
                    read_seconds = perf_counter() - started
                    timings["leitura"] += read_seconds
                    logger.info(
                        "Busca PostgreSQL: bloco %s/%s; %s linhas; %.3fs",
                        block_index, len(project_blocks), len(rows), read_seconds,
                    )
                    if not rows:
                        break
                    started = perf_counter()
                    columns = [column[0] for column in cur.description]
                    chunk = pd.DataFrame.from_records(rows, columns=columns)
                    chunk = apply_contrato_mapping(chunk, args)
                    chunk["colaborador_salario"] = pd.to_numeric(
                        chunk["colaborador_salario"], errors="raise",
                    ).astype("float64")
                    after_dataframe = perf_counter()
                    table = pa.Table.from_pandas(
                        chunk, schema=PARQUET_SCHEMA, preserve_index=False, safe=True,
                    ).replace_schema_metadata(None)
                    after_arrow = perf_counter()
                    parquet_writer.write_table(table)
                    finished = perf_counter()
                    timings["dataframe"] += after_dataframe - started
                    timings["arrow"] += after_arrow - after_dataframe
                    timings["escrita"] += finished - after_arrow
                    block_rows += len(chunk)
                    total += len(chunk)
                    logger.info(
                        "Registros exportados: %d; lote: %s; dataframe: %.3fs; Arrow: %.3fs; escrita: %.3fs",
                        total, len(chunk), after_dataframe - started,
                        after_arrow - after_dataframe, finished - after_arrow,
                    )
            logger.info(
                "Bloco %s/%s concluido: %s linhas; total: %.3fs; buscas PostgreSQL: %.3fs",
                block_index, len(project_blocks), block_rows,
                perf_counter() - block_started, timings["leitura"] - block_read_started,
            )

        if total == 0:
            logger.warning("Consulta nao retornou registros. Parquet vazio gerado com schema fixo.")
        logger.info("Parquet gerado com sucesso: %s", out_path)

    finally:
        if parquet_writer:
            parquet_writer.close()
        conn.close()

    logger.info("Total de registros: %d", total)
    logger.info(
        "Tempos acumulados: leitura PostgreSQL: %.3fs; dataframe: %.3fs; Arrow: %.3fs; escrita Parquet: %.3fs",
        timings["leitura"], timings["dataframe"], timings["arrow"], timings["escrita"],
    )


def main() -> int:
    try:
        args = parse_args()
        out_path = ensure_output_path(args.out)
        export_to_parquet(args, out_path)
        return 0
    except Exception as e:
        logger.exception("Falha: %s", e)
        return 1


if __name__ == "__main__":
    from migracao_logging import run_script

    raise SystemExit(run_script(main, __file__))
