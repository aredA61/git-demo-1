package com.gzcb.creditcard.service.gameCenter.activity.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.gzcb.creditcard.common.base.RequestData;
import com.gzcb.creditcard.common.base.ResponseCode;
import com.gzcb.creditcard.common.base.ResponseData;
import com.gzcb.creditcard.common.constants.Constants;
import com.gzcb.creditcard.common.constants.QualificationTypeEnum;
import com.gzcb.creditcard.common.constants.RedisKeyPrefix;
import com.gzcb.creditcard.common.constants.audit.AuditConstans;
import com.gzcb.creditcard.common.dto.gameCenter.activity.*;
import com.gzcb.creditcard.common.dto.gameCenter.activity.turntable.*;
import com.gzcb.creditcard.common.model.audit.AuditTaskDo;
import com.gzcb.creditcard.common.model.gameCenter.activity.GameActivityDo;
import com.gzcb.creditcard.common.model.gameCenter.gameActivityCount.GameActivityCountDo;
import com.gzcb.creditcard.common.model.gameCenter.reward.GameRewardDo;
import com.gzcb.creditcard.common.model.gameCenter.rewardPool.GameRewardPoolDo;
import com.gzcb.creditcard.common.model.gameCenter.rewardRel.GameRewardRelDo;
import com.gzcb.creditcard.common.model.gameCenter.template.GameTemplateDo;
import com.gzcb.creditcard.common.model.gameCenter.templateReward.GameTemplateRewardDo;
import com.gzcb.creditcard.common.model.qualifyJudgeParam.QualifyJudgeParamDo;
import com.gzcb.creditcard.common.util.DateUtil;
import com.gzcb.creditcard.common.util.RedisUtil;
import com.gzcb.creditcard.common.util.StringUtil;
import com.gzcb.creditcard.mapper.activityVersion.ActivityVersionRelMapper;
import com.gzcb.creditcard.mapper.audit.AuditTaskMapper;
import com.gzcb.creditcard.mapper.gameCenter.activity.GameActivityMapper;
import com.gzcb.creditcard.mapper.gameCenter.gameActivityCount.GameActivityCountMapper;
import com.gzcb.creditcard.mapper.gameCenter.reward.RewardMapper;
import com.gzcb.creditcard.mapper.gameCenter.rewardPool.RewardPoolMapper;
import com.gzcb.creditcard.mapper.gameCenter.rewardRel.RewardRelMapper;
import com.gzcb.creditcard.mapper.gameCenter.template.GameTemplateMapper;
import com.gzcb.creditcard.mapper.gameCenter.templateReward.GameTemplateRewardMapper;
import com.gzcb.creditcard.mapper.qualifyJudgeParam.QualifyJudgeParamMapper;
import com.gzcb.creditcard.model.activityVersion.ActivityVersionRel;
import com.gzcb.creditcard.service.audit.AuditTaskService;
import com.gzcb.creditcard.service.gameCenter.activity.GameActivityTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author: YuanZhiZheng
 * @time: 2021/10/12 17:07
 */
@Slf4j
@Service
public class GameActivityTransactionServiceImpl implements GameActivityTransactionService {

    @Autowired
    private GameActivityMapper gameActivityMapper;

    @Autowired
    private GameTemplateMapper gameTemplateMapper;

    @Autowired
    private RewardPoolMapper rewardPoolMapper;

    @Autowired
    private RewardMapper rewardMapper;

    @Autowired
    private GameTemplateRewardMapper gameTemplateRewardMapper;

    @Autowired
    private RewardRelMapper rewardRelMapper;

    @Autowired
    private QualifyJudgeParamMapper qualifyJudgeParamMapper;

    @Autowired
    private GameActivityCountMapper gameActivityCountMapper;

    @Resource
    private AuditTaskService auditTaskService;

    @Resource
    private ActivityVersionRelMapper activityVersionRelMapper;

    @Resource
    private AuditTaskMapper auditTaskMapper;

    @Override
    @Transactional(timeout= Constants.TRANSACTIONAL_TIMEOUT)
    public ResponseData insertActivityDB(RequestData<GameActivityDto> requestData) {
        log.info("新增活动:{}",requestData);
        GameActivityDto gameActivityDto = JSON.parseObject(JSON.toJSONString(requestData.getContent()), GameActivityDto.class);
        try {
//            gameActivityDto.setGameStatus(Constants.GAME_STATUS_DOWN); //活动状态默认为下架
            //校验数据
             ResponseData responseData = checkData(gameActivityDto);
            if (ResponseCode.OK.getCode() != responseData.getCode()) {
                return responseData;
            }

            //插入资格判断记录
            QualifyJudgeParamDo qualifyJudgeParamDo = null;
            if (!StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) || !StringUtil.isEmpty(gameActivityDto.getMktNameId())) {
                qualifyJudgeParamDo = new QualifyJudgeParamDo();
                qualifyJudgeParamDo.setJudgeType(this.getJudgeType(gameActivityDto.getMktRightsCode(), gameActivityDto.getMktNameId()));
                qualifyJudgeParamDo.setMktRightsCode(gameActivityDto.getMktRightsCode());
                qualifyJudgeParamDo.setMktNameId(gameActivityDto.getMktNameId());
                qualifyJudgeParamDo.setCreateOper(gameActivityDto.getCreateOper());
                qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                qualifyJudgeParamDo.setDeptId(gameActivityDto.getDeptId());
                qualifyJudgeParamMapper.insertQualifyJudgeParam(qualifyJudgeParamDo);
            }

            //插入游戏主表数据
            GameActivityDo gameActivityDo = new GameActivityDo();
            gameActivityDo.setGameName(gameActivityDto.getGameName());
            gameActivityDo.setGameTemplateType(gameActivityDto.getGameTemplateType());
            gameActivityDo.setStartTime(gameActivityDto.getStartTime());
            gameActivityDo.setEndTime(gameActivityDto.getEndTime());
            gameActivityDo.setRosterRid(gameActivityDto.getRosterRid());
            gameActivityDo.setEnableWhiteRoster(gameActivityDto.getEnableWhiteRoster());
            if (qualifyJudgeParamDo != null) {
                gameActivityDo.setFocusJudgeId(qualifyJudgeParamDo.getId());
            }
            gameActivityDo.setIsEnableBkupPool(gameActivityDto.getIsEnableBkupPool());
            gameActivityDo.setAmountTotalLimit(gameActivityDto.getAmountTotalLimit());
            gameActivityDo.setAmountAnnuallyLimit(gameActivityDto.getAmountAnnuallyLimit());
            gameActivityDo.setAmountMonthlyLimit(gameActivityDto.getAmountMonthlyLimit());
            gameActivityDo.setAmountPersonalTotalLimit(gameActivityDto.getAmountPersonalTotalLimit());
            gameActivityDo.setAmountPersonalMonthlyLimit(gameActivityDto.getAmountPersonalMonthlyLimit());
            gameActivityDo.setAmountPersonalYearlyLimit(gameActivityDto.getAmountPersonalYearlyLimit());
            gameActivityDo.setIsEnableHighReward(gameActivityDto.getIsEnableHighReward());
            gameActivityDo.setHighRewardDays(gameActivityDto.getHighRewardDays());
            gameActivityDo.setHighRewardTimes(gameActivityDto.getHighRewardTimes());
            gameActivityDo.setHighRewardAmount(gameActivityDto.getHighRewardAmount());
            gameActivityDo.setCreateOper(gameActivityDto.getCreateOper());
            gameActivityDo.setModifyOper(gameActivityDto.getModifyOper());
            gameActivityDo.setDeptId(gameActivityDto.getDeptId());
            gameActivityDo.setShelfStatus(Constants.GAME_STATUS_DOWN); //默认为下架状态
            gameActivityMapper.insertGameActivity(gameActivityDo);

            //插入游戏模板数据
            ConfigContentDto configContentDto = JSON.parseObject(JSON.toJSONString(gameActivityDto.getConfigContent(),SerializerFeature.WriteNullListAsEmpty), ConfigContentDto.class);
            //一次遍历将展示模板里有关资格判断的相关数据插入资格判断参数表（t_qualify_judge_param）之中，
            //并且将返回插入的对应的数据依次设置到展示模板里。
            List<ExhibitionTemplate> templates = null;//这里设置模板为空，应为只有天天返现专区增加了展示设置。其他模板创建时，应没有模板
            if (configContentDto.getTemplates() != null && configContentDto.getTemplates().size() > 0){
                templates = configContentDto.getTemplates();
                QualifyJudgeParamDo qualifyJudgeParam = null;
                for (ExhibitionTemplate exhibitionTemplate : templates) {
                    if(StringUtil.isEmpty(exhibitionTemplate.getPopUpSeconds())){
                        exhibitionTemplate.setPopUpSeconds("3");
                    }
                    if(!"0".equals(exhibitionTemplate.getTemplatePriority())){
                        qualifyJudgeParam = new QualifyJudgeParamDo();
                        qualifyJudgeParam.setJudgeType(exhibitionTemplate.getJudgeType());
                        qualifyJudgeParam.setMktRightsCode(exhibitionTemplate.getMktRightsCode());
                        qualifyJudgeParam.setMktRightsMonthType(exhibitionTemplate.getMktRightsMonthType());
                        qualifyJudgeParam.setMktRightsMonthNum(exhibitionTemplate.getMktRightsMonthNum());
                        qualifyJudgeParam.setMktRightsStatus(exhibitionTemplate.getMktRightsStatus());
                        qualifyJudgeParam.setMktRightsRecordType(exhibitionTemplate.getMktRightsRecordType());
                        qualifyJudgeParam.setMktActivityId(exhibitionTemplate.getMktActivityId());
                        qualifyJudgeParam.setMktPlanId(exhibitionTemplate.getMktPlanId());
                        setTemplateQualifyJudgeNull(exhibitionTemplate);
                        qualifyJudgeParamMapper.insertQualifyJudgeParam(qualifyJudgeParam);
                        exhibitionTemplate.setQualifyJudgeId(qualifyJudgeParam.getId());
                    }
                }
            }
            //将不带权益资格判断的模板数据设置回ConfigContentDto,当前端没有templates数据时此时依然为空
            configContentDto.setTemplates(templates);

            //新增游戏模板
            GameTemplateDo gameTemplateDo = new GameTemplateDo();
            gameTemplateDo.setGameId(gameActivityDo.getId());
            gameTemplateDo.setLimitRewardNum(gameActivityDto.getLimitRewardNum());
            gameTemplateDo.setLimitRewardType(gameActivityDto.getLimitRewardType());
            gameTemplateDo.setLimitPoolNum(gameActivityDto.getLimitPoolNum());
            gameTemplateDo.setConfigContent(JSON.toJSONString(configContentDto, SerializerFeature.WriteNullListAsEmpty));
            gameTemplateDo.setCreateOper(gameActivityDto.getCreateOper());
            gameTemplateDo.setModifyOper(gameActivityDto.getModifyOper());
            gameTemplateDo.setDeptId(gameActivityDto.getDeptId());
            gameTemplateMapper.insertGameTemplate(gameTemplateDo);


            List<GameRewardInfoDto> rewardInfos = configContentDto.getRewardInfoList();
            //游戏模板奖品列表
            ArrayList<GameTemplateRewardDo> gameTemplateRewardDos = new ArrayList<>();
            if (rewardInfos != null && rewardInfos.size() > 0) {
                for (GameRewardInfoDto gameRewardInfoDto : rewardInfos) {
                    //插入游戏模板奖励数据
                    GameTemplateRewardDo gameTemplateRewardDo = new GameTemplateRewardDo();
                    gameTemplateRewardDo.setGameId(gameActivityDo.getId());
                    gameTemplateRewardDo.setGameTemplateId(gameTemplateDo.getId());
                    gameTemplateRewardDo.setRewardName(gameRewardInfoDto.getRewardName());
                    gameTemplateRewardDo.setPicUrl(gameRewardInfoDto.getPicUrl());
                    gameTemplateRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                    gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                    gameTemplateRewardDo.setDeptId(gameActivityDto.getDeptId());

                    gameTemplateRewardMapper.insertGameTemplateReward(gameTemplateRewardDo);
                    gameTemplateRewardDos.add(gameTemplateRewardDo);
                }
            }

            //奖励池列表
            List<GameRewardPoolDto> poolList = gameActivityDto.getPoolList();
            List<GameRewardPoolDo> mainPools = savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, poolList);
            if (mainPools == null) {
                throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
            }
            ArrayList<Long> mainPoolIdList = new ArrayList<>();
            for (GameRewardPoolDo gameRewardPoolDo : mainPools) {
                mainPoolIdList.add(gameRewardPoolDo.getId());
            }
            if ("1".equals(gameActivityDto.getIsEnableBkupPool())) {
                //开启备用奖池
                BackupPoolListDto backupPoolList = gameActivityDto.getBackupPoolList();
                if (backupPoolList == null) {
                    throw new InvalidParameterException("开启备用奖池但是备用奖池数据为空");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "开启备用奖池但是备用奖池数据为空");
                }
                List<GameRewardPoolDto> amountTotalPools = backupPoolList.getAmountTotalPools();//刷卡金奖励总限额备用奖池
                List<GameRewardPoolDto> amountAnnuallyPools = backupPoolList.getAmountAnnuallyPools();//刷卡金奖励年度限额备用奖池
                List<GameRewardPoolDto> amountMonthlyPools = backupPoolList.getAmountMonthlyPools();//刷卡金奖励月度限额备用奖池
                List<GameRewardPoolDto> amountPersonalTotalPools = backupPoolList.getAmountPersonalTotalPools();//刷卡金个人总限额备用奖池
                List<GameRewardPoolDto> amountPersonalMonthlyPools = backupPoolList.getAmountPersonalMonthlyPools();//刷卡金个人月度限额备用奖池
                List<GameRewardPoolDto> amountPersonalYearlyPools = backupPoolList.getAmountPersonalYearlyPools();//刷卡金个人年度限额备用奖池
                //设置主奖池id
                for (int i = 0; i < mainPoolIdList.size(); i++) {
                    if (amountTotalPools != null && amountTotalPools.size() > 0) {
                        amountTotalPools.get(i).setMainPoolId(mainPoolIdList.get(i));
                    }
                    if (amountAnnuallyPools != null && amountAnnuallyPools.size() > 0) {
                        amountAnnuallyPools.get(i).setMainPoolId(mainPoolIdList.get(i));
                    }
                    if (amountMonthlyPools != null && amountMonthlyPools.size() > 0) {
                        amountMonthlyPools.get(i).setMainPoolId(mainPoolIdList.get(i));
                    }
                    if (amountPersonalTotalPools != null && amountPersonalTotalPools.size() > 0) {
                        amountPersonalTotalPools.get(i).setMainPoolId(mainPoolIdList.get(i));
                    }
                    if (amountPersonalMonthlyPools != null && amountPersonalMonthlyPools.size() > 0) {
                        amountPersonalMonthlyPools.get(i).setMainPoolId(mainPoolIdList.get(i));
                    }
                    if (amountPersonalYearlyPools != null && amountPersonalYearlyPools.size() > 0) {
                        amountPersonalYearlyPools.get(i).setMainPoolId(mainPoolIdList.get(i));
                    }
                }
                if (savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, amountTotalPools) == null) {
                    throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
                }
                if (savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, amountAnnuallyPools) == null) {
                    throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
                }
                if (savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, amountMonthlyPools) == null) {
                    throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
                }
                if (savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, amountPersonalTotalPools) == null) {
                    throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
                }
                if (savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, amountPersonalMonthlyPools) == null) {
                    throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
                }
                if (savePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, amountPersonalYearlyPools) == null) {
                    throw new InvalidParameterException("模板奖励数量与奖池奖励数量不一致");
//                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励数量与奖池奖励数量不一致");
                }
            }
            //t_game_activity_count表创建当天和明天的记录
            String[] dateTimes = {DateUtil.getyyyyMMdd(), DateUtil.getTimeStr("yyyyMMdd", 1)};//当天、明天
            for (String dateTime : dateTimes) {
                GameActivityCountDo activityCountQuery = GameActivityCountDo.builder().gameId(gameActivityDo.getId()).dateTime(dateTime).build();
                List<GameActivityCountDo> activityCountList = gameActivityCountMapper.list(activityCountQuery);
                // 不存在记录
                if (CollectionUtils.isEmpty(activityCountList)) {
                    GameActivityCountDo insertActivityCount = GameActivityCountDo.builder()
                            .gameId(gameActivityDo.getId())
                            .dateTime(dateTime)
                            .receivedAmount(BigDecimal.ZERO)
                            .receivedIntegral(0)
                            .receivedNum(0)
                            .build();
                    gameActivityCountMapper.save(insertActivityCount);
                }
            }
            log.info("新增活动成功");

            // 关联活动版本
            activityVersion(gameActivityDo.getId(),gameActivityDto.getId(),gameActivityDto.getVersionId(),gameActivityDto.getNewVersionFlag());
            return new ResponseData<>(ResponseCode.OK, gameActivityDo.getId());
        }catch (InvalidParameterException e) {
            log.error(e.getMessage());
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.CODE_403.getCode(), e.getMessage());
        }catch (Exception e){
            log.error("新增活动异常",e);
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.ERROR.getCode(), ResponseCode.ERROR.getMessage());
        }
    }

    /**
     * 关联活动版本
     *
     * @param newId              活动配置主键
     * @param oldId             新建版本时旧活动配置主键
     * @param versionId         当前活动版本号
     * @param newVersionFlag    是否迭代版本号
     */
    private void activityVersion(Long newId, Long oldId, Integer versionId, Integer newVersionFlag){
        // 初始版本
        ActivityVersionRel activityVersionRel = ActivityVersionRel.builder()
                .activityId(newId)
                .activityType(Constants.ACTIVITY_VERSION_TYPE_GAME_CENTER)
                .versionId(1)
                .configId(newId)
                .build();
        // 新增版本
        if (Constants.TRUE_INTEGER.equals(newVersionFlag)){
            // 查询当前活动最新版本号
            ActivityVersionRel newlyVersion = activityVersionRelMapper.selectNewlyVersion(oldId, versionId);
            log.info("最新版本活动活动：{}",newlyVersion);
            activityVersionRel.setVersionId(newlyVersion.getVersionId()+1);
            activityVersionRel.setActivityId(newlyVersion.getActivityId());
        }
        activityVersionRelMapper.insert(activityVersionRel);
    }


    @Override
    @Transactional(timeout= Constants.TRANSACTIONAL_TIMEOUT)
    public ResponseData updateActivityDB(RequestData<GameActivityDto> requestData) {
        log.info("修改活动:{}",requestData);
        GameActivityDto gameActivityDto = JSON.parseObject(JSON.toJSONString(requestData.getContent()), GameActivityDto.class);
        try{
            if(gameActivityDto == null || gameActivityDto.getId() == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动id不能为空");
            }

            GameActivityDo gameActivityDo = gameActivityMapper.queryById(gameActivityDto.getId());
            if (gameActivityDo == null) {
                //获取不到活动信息
                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_ACTIVITY_NOT_EXIST);
            }
            Date now = new Date();
            if (Constants.GAME_STATUS_UP.equals(gameActivityDo.getShelfStatus()) && now.before(gameActivityDo.getEndTime()) && now.after(gameActivityDo.getStartTime())) {
                //活动生效时间内为上架状态，无法修改
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动生效时间内为上架状态，无法修改");
            }

            //如果游戏更新检查任务的名称与 游戏名称是否相同，不相同修该
            auditTaskService.updateTaskName(gameActivityDto.getGameName(),gameActivityDto.getId(),AuditConstans.AUDIT_CONTENT_TYPE_ACTIVITY_CENTER);


            //校验数据
            ResponseData responseData = checkData(gameActivityDto);
            if(ResponseCode.OK.getCode() != responseData.getCode()){
                return responseData;
            }
            if("0".equals(gameActivityDto.getIsEnableBkupPool())){
                //不启用备用奖池，设置五个限额为null
                gameActivityDto.setAmountTotalLimit(null);
                gameActivityDto.setAmountAnnuallyLimit(null);
                gameActivityDto.setAmountMonthlyLimit(null);
                gameActivityDto.setAmountPersonalTotalLimit(null);
                gameActivityDto.setAmountPersonalMonthlyLimit(null);
                gameActivityDo.setAmountPersonalYearlyLimit(null);
            }
            if("0".equals(gameActivityDto.getIsEnableHighReward())){
                //不启用高奖励限制
                gameActivityDto.setHighRewardDays(null);
                gameActivityDto.setHighRewardTimes(null);
                gameActivityDto.setHighRewardAmount(null);
            }
            GameActivityDo gameActivityDo1 = new GameActivityDo();
            gameActivityDo1.setId(gameActivityDo.getId());
            gameActivityDo1.setGameName(gameActivityDto.getGameName());
            gameActivityDo1.setStartTime(gameActivityDto.getStartTime());
            gameActivityDo1.setEndTime(gameActivityDto.getEndTime());
            gameActivityDo1.setIsEnableBkupPool(gameActivityDto.getIsEnableBkupPool());
            gameActivityDo1.setAmountTotalLimit(gameActivityDto.getAmountTotalLimit());
            gameActivityDo1.setAmountAnnuallyLimit(gameActivityDto.getAmountAnnuallyLimit());
            gameActivityDo1.setAmountMonthlyLimit(gameActivityDto.getAmountMonthlyLimit());
            gameActivityDo1.setAmountPersonalTotalLimit(gameActivityDto.getAmountPersonalTotalLimit());
            gameActivityDo1.setAmountPersonalMonthlyLimit(gameActivityDto.getAmountPersonalMonthlyLimit());
            gameActivityDo1.setAmountPersonalYearlyLimit(gameActivityDto.getAmountPersonalYearlyLimit());
            gameActivityDo1.setIsEnableHighReward(gameActivityDto.getIsEnableHighReward());
            gameActivityDo1.setHighRewardDays(gameActivityDto.getHighRewardDays());
            gameActivityDo1.setHighRewardTimes(gameActivityDto.getHighRewardTimes());
            gameActivityDo1.setHighRewardAmount(gameActivityDto.getHighRewardAmount());
            gameActivityDo1.setModifyOper(gameActivityDto.getModifyOper());
            gameActivityDo1.setShelfStatus(Constants.GAME_STATUS_DOWN); //修改后需要手动上线，默认为下架状态
            gameActivityDo1.setEnableWhiteRoster(gameActivityDto.getEnableWhiteRoster());
            gameActivityDo1.setRosterRid(gameActivityDto.getRosterRid());
            //更新资格判断记录
            QualifyJudgeParamDo qualifyJudgeParamDo = null;
            if(gameActivityDo.getFocusJudgeId() != null){
                qualifyJudgeParamDo = qualifyJudgeParamMapper.queryById(gameActivityDo.getFocusJudgeId());
                if(qualifyJudgeParamDo == null){
                    //资格判断信息不存在
                    return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_QUALIFY_JUDGE_PARAM_NOT_EXIST);
                }
                if(StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) && StringUtil.isEmpty(gameActivityDto.getMktNameId())){
                    //删除资格判断记录
                    qualifyJudgeParamDo.setIsDel("1");
                    qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                    qualifyJudgeParamMapper.delQualifyJudgeParam(qualifyJudgeParamDo);
                    gameActivityDo1.setFocusJudgeId(null);
                }else{
                    //更新资格判断记录
                    QualifyJudgeParamDo updateQualifyJudgeParam = new QualifyJudgeParamDo();
                    updateQualifyJudgeParam.setId(qualifyJudgeParamDo.getId());
                    updateQualifyJudgeParam.setJudgeType(this.getJudgeType(gameActivityDto.getMktRightsCode(),gameActivityDto.getMktNameId()));
                    updateQualifyJudgeParam.setMktRightsCode(gameActivityDto.getMktRightsCode()==null?"":gameActivityDto.getMktRightsCode());
                    updateQualifyJudgeParam.setMktNameId(gameActivityDto.getMktNameId()==null?"":gameActivityDto.getMktNameId());
                    updateQualifyJudgeParam.setModifyOper(gameActivityDto.getModifyOper());
                    qualifyJudgeParamMapper.updateQualifyJudgeParam(updateQualifyJudgeParam);
                    gameActivityDo1.setFocusJudgeId(updateQualifyJudgeParam.getId());
                }
            }else{
                if(!StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) || !StringUtil.isEmpty(gameActivityDto.getMktNameId())){
                    //新增资格判断记录
                    qualifyJudgeParamDo = new QualifyJudgeParamDo();
                    qualifyJudgeParamDo.setJudgeType(this.getJudgeType(gameActivityDto.getMktRightsCode(),gameActivityDto.getMktNameId()));
                    qualifyJudgeParamDo.setMktRightsCode(gameActivityDto.getMktRightsCode());
                    qualifyJudgeParamDo.setMktNameId(gameActivityDto.getMktNameId());
                    qualifyJudgeParamDo.setCreateOper(gameActivityDto.getCreateOper());
                    qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                    qualifyJudgeParamDo.setDeptId(gameActivityDto.getDeptId());
                    qualifyJudgeParamMapper.insertQualifyJudgeParam(qualifyJudgeParamDo);
                    gameActivityDo1.setFocusJudgeId(qualifyJudgeParamDo.getId());
                }
            }
            //更新活动
            gameActivityMapper.updateGameActivity(gameActivityDo1);

            GameTemplateDo gameTemplateDo = gameTemplateMapper.queryByGameId(gameActivityDto.getId());
            if(gameActivityDo == null){
                //获取不到活动模板信息
                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_TEMPLATE_NOT_EXIST);
            }

            List<GameTemplateRewardDo> gameTemplateRewardDos = gameTemplateRewardMapper.queryByGameIdAndGameTemplateId(gameActivityDo.getId(), gameTemplateDo.getId());

            ConfigContentDto configContentDto = gameActivityDto.getConfigContent();
            List<GameRewardInfoDto> rewardInfos = configContentDto.getRewardInfoList();
            List<GameRewardInfoDto> rewardInfoList = new ArrayList<>();
            //游戏模板奖品列表
            ArrayList<GameTemplateRewardDo> gameTemplateRewardDoList = new ArrayList<>();
            ArrayList<GameTemplateRewardDo> gameTemplateRewardDoUpdateList = new ArrayList<>();
            if(rewardInfos != null && rewardInfos.size() > 0){
                for (GameRewardInfoDto gameRewardInfoDto :rewardInfos) {
                    GameTemplateRewardDo gameTemplateRewardDo = new GameTemplateRewardDo();
                    if(gameRewardInfoDto.getId() == null ){
                        //插入游戏模板奖励数据
                        gameTemplateRewardDo.setGameId(gameActivityDo.getId());
                        gameTemplateRewardDo.setGameTemplateId(gameTemplateDo.getId());
                        gameTemplateRewardDo.setRewardName(gameRewardInfoDto.getRewardName());
                        gameTemplateRewardDo.setPicUrl(gameRewardInfoDto.getPicUrl());
                        gameTemplateRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                        gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                        gameTemplateRewardDo.setDeptId(gameActivityDto.getDeptId());


                        gameTemplateRewardMapper.insertGameTemplateReward(gameTemplateRewardDo);
                        gameTemplateRewardDoList.add(gameTemplateRewardDo);
                        GameRewardInfoDto rewardInfoTmp = new GameRewardInfoDto();
                        rewardInfoTmp.setId(gameTemplateRewardDo.getId());
                        rewardInfoTmp.setRewardName(gameTemplateRewardDo.getRewardName());
                        rewardInfoTmp.setPicUrl(gameTemplateRewardDo.getPicUrl());
                        rewardInfoTmp.setIsDel("0");
                        rewardInfoList.add(rewardInfoTmp);
                    }else{
                        if("1".equals(gameRewardInfoDto.getIsDel())){
                            //删除
                            gameTemplateRewardDo.setId(gameRewardInfoDto.getId());
                            gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            gameTemplateRewardDo.setIsDel("1");
                            gameTemplateRewardMapper.delGameTemplateReward(gameTemplateRewardDo);
                        }else{
                            //修改
                            gameTemplateRewardDo.setRewardName(gameRewardInfoDto.getRewardName());
                            gameTemplateRewardDo.setPicUrl(gameRewardInfoDto.getPicUrl());
                            gameTemplateRewardDo.setId(gameRewardInfoDto.getId());
                            gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());

                            gameTemplateRewardMapper.updateGameTemplateReward(gameTemplateRewardDo);
                            gameTemplateRewardDoList.add(gameTemplateRewardDo);
                            rewardInfoList.add(gameRewardInfoDto);
                        }
                    }
                    gameTemplateRewardDoUpdateList.add(gameTemplateRewardDo);
                }
            }


            //删除表中的分众
            String configContent = gameTemplateDo.getConfigContent();
            ConfigContentDto configContentDo = JSON.parseObject(configContent, ConfigContentDto.class);
            List<ExhibitionTemplate> templates = configContentDo.getTemplates();
            List<Long> idList = new ArrayList<>();
            if (templates != null && templates.size() > 0){
                for (ExhibitionTemplate template: templates) {
                    if(template != null){
                        if (!"0".equals(template.getTemplatePriority())){
                            Long qualifyJudgeId = template.getQualifyJudgeId();
                            idList.add(qualifyJudgeId);
                        }
                    }
                }
                if (idList != null && idList.size() > 0){
                    qualifyJudgeParamMapper.batchDel(idList,gameActivityDto.getModifyOper());
                }
            }

            //添加展示节点模板中的权益判断信息（添加分众）
            List<ExhibitionTemplate> templateList = configContentDto.getTemplates();
            if (templateList != null && templateList.size() > 0){
                for (ExhibitionTemplate template: templateList) {
                    if(StringUtil.isEmpty(template.getTemplatePriority())){
                        throw new InvalidParameterException("模板展示优先级不能为空");
                    }
                    if (template != null){
                        if(!"0".equals(template.getTemplatePriority())){
                            QualifyJudgeParamDo qualifyJudgeParamDo1 = new QualifyJudgeParamDo();
                            qualifyJudgeParamDo1.setJudgeType(template.getJudgeType());
                            qualifyJudgeParamDo1.setMktRightsCode(template.getMktRightsCode());
                            qualifyJudgeParamDo1.setMktRightsMonthType(template.getMktRightsMonthType());
                            qualifyJudgeParamDo1.setMktRightsMonthNum(template.getMktRightsMonthNum());
                            qualifyJudgeParamDo1.setMktRightsStatus(template.getMktRightsStatus());
                            qualifyJudgeParamDo1.setMktRightsRecordType(template.getMktRightsRecordType());
                            qualifyJudgeParamDo1.setMktActivityId(template.getMktActivityId());
                            qualifyJudgeParamDo1.setMktPlanId(template.getMktPlanId());
                            setTemplateQualifyJudgeNull(template);
                            qualifyJudgeParamMapper.insertQualifyJudgeParam(qualifyJudgeParamDo1);
                            template.setQualifyJudgeId(qualifyJudgeParamDo1.getId());
                        }
                    }
                }
            }


            //更新游戏模板
            ConfigContentDto configContentTmp = new ConfigContentDto();
            configContentTmp.setIndexShowContent(configContentDto.getIndexShowContent());
            configContentTmp.setPageTitle(configContentDto.getPageTitle());
            configContentTmp.setGameRule(configContentDto.getGameRule());
            configContentTmp.setMainView(configContentDto.getMainView());
            configContentTmp.setRewardInfoList(rewardInfoList);
            configContentTmp.setTemplates(templateList);


            GameTemplateDo gameTemplateDo1 = new GameTemplateDo();
            BeanUtils.copyProperties(gameActivityDto,gameTemplateDo1);
            gameTemplateDo1.setId(gameTemplateDo.getId());
            //删除展示模板中的资格判断有关数据
            gameTemplateDo1.setConfigContent(JSON.toJSONString(configContentTmp,SerializerFeature.WriteNullListAsEmpty,SerializerFeature.WriteNullStringAsEmpty));
            gameTemplateMapper.updateGameTemplate(gameTemplateDo1);

            //主奖池
            List<GameRewardPoolDto> poolList = gameActivityDto.getPoolList();

            List<GameRewardPoolDo> mainPool = poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, poolList, null);
            if("0".equals(gameActivityDto.getIsEnableBkupPool())){
                GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                backUpPoolQuery.setGameId(gameActivityDo.getId());
                backUpPoolQuery.setIsBackup("1");
                backUpPoolQuery.setIsDel("0");
                List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
            }else{
                //备用奖池
                if(gameActivityDto.getAmountTotalLimit() == null){
                    GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                    backUpPoolQuery.setGameId(gameActivityDo.getId());
                    backUpPoolQuery.setIsBackup("1");
                    backUpPoolQuery.setBackupPoolType("1");
                    backUpPoolQuery.setIsDel("0");
                    List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                    delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
                }
                if(gameActivityDto.getAmountAnnuallyLimit() == null){
                    GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                    backUpPoolQuery.setGameId(gameActivityDo.getId());
                    backUpPoolQuery.setIsBackup("1");
                    backUpPoolQuery.setBackupPoolType("2");
                    backUpPoolQuery.setIsDel("0");
                    List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                    delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
                }
                if(gameActivityDto.getAmountMonthlyLimit() == null){
                    GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                    backUpPoolQuery.setGameId(gameActivityDo.getId());
                    backUpPoolQuery.setIsBackup("1");
                    backUpPoolQuery.setBackupPoolType("3");
                    backUpPoolQuery.setIsDel("0");
                    List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                    delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
                }
                if(gameActivityDto.getAmountPersonalTotalLimit() == null){
                    GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                    backUpPoolQuery.setGameId(gameActivityDo.getId());
                    backUpPoolQuery.setIsBackup("1");
                    backUpPoolQuery.setBackupPoolType("4");
                    backUpPoolQuery.setIsDel("0");
                    List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                    delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
                }
                if(gameActivityDto.getAmountPersonalMonthlyLimit() == null){
                    GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                    backUpPoolQuery.setGameId(gameActivityDo.getId());
                    backUpPoolQuery.setIsBackup("1");
                    backUpPoolQuery.setBackupPoolType("5");
                    backUpPoolQuery.setIsDel("0");
                    List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                    delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
                }
                //删除个人年度总限额奖池数据
                if(gameActivityDto.getAmountPersonalYearlyLimit() == null){
                    GameRewardPoolDo backUpPoolQuery = new GameRewardPoolDo();
                    backUpPoolQuery.setGameId(gameActivityDo.getId());
                    backUpPoolQuery.setIsBackup("1");
                    backUpPoolQuery.setBackupPoolType("6");
                    backUpPoolQuery.setIsDel("0");
                    List<GameRewardPoolDo> backUpPools = rewardPoolMapper.queryByRewardPool(backUpPoolQuery);
                    delBackUpPool(gameActivityDto, gameActivityDo, backUpPools);
                }
                BackupPoolListDto backupPoolList = gameActivityDto.getBackupPoolList();
                if(backupPoolList != null){
                    List<GameRewardPoolDto> amountTotalPools = backupPoolList.getAmountTotalPools();//刷卡金奖励总限额备用奖池
                    List<GameRewardPoolDto> amountAnnuallyPools = backupPoolList.getAmountAnnuallyPools();//刷卡金奖励年度限额备用奖池
                    List<GameRewardPoolDto> amountMonthlyPools = backupPoolList.getAmountMonthlyPools();//刷卡金奖励月度限额备用奖池
                    List<GameRewardPoolDto> amountPersonalTotalPools = backupPoolList.getAmountPersonalTotalPools();//刷卡金个人总限额备用奖池
                    List<GameRewardPoolDto> amountPersonalMonthlyPools = backupPoolList.getAmountPersonalMonthlyPools();//刷卡金个人月度限额备用奖池
                    List<GameRewardPoolDto> amountPersonalYearlyPools = backupPoolList.getAmountPersonalYearlyPools();//刷卡金个人年度限额备用奖池
                    if(gameActivityDto.getAmountTotalLimit() != null && amountTotalPools != null && amountTotalPools.size() > 0){
                        poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, amountTotalPools,mainPool);
                    }
                    if(gameActivityDto.getAmountAnnuallyLimit() != null && amountAnnuallyPools != null && amountAnnuallyPools.size() > 0){
                        poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, amountAnnuallyPools,mainPool);
                    }
                    if(gameActivityDto.getAmountMonthlyLimit() != null && amountMonthlyPools != null && amountMonthlyPools.size() > 0){
                        poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, amountMonthlyPools,mainPool);
                    }
                    if(gameActivityDto.getAmountPersonalTotalLimit() != null && amountPersonalTotalPools != null && amountPersonalTotalPools.size() > 0){
                        poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, amountPersonalTotalPools,mainPool);
                    }
                    if(gameActivityDto.getAmountPersonalMonthlyLimit() != null && amountPersonalMonthlyPools != null && amountPersonalMonthlyPools.size() > 0){
                        poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, amountPersonalMonthlyPools,mainPool);
                    }
                    if(gameActivityDto.getAmountPersonalYearlyLimit() != null && amountPersonalYearlyPools != null && amountPersonalYearlyPools.size() > 0){
                        poolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, amountPersonalYearlyPools,mainPool);
                    }
                }
            }

            return new ResponseData(ResponseCode.OK,gameActivityDo.getId());
        } catch (IllegalArgumentException e) {
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.CODE_403.getCode(), e.getMessage());
        }
        catch (Exception e){
            log.error("修改活动异常",e);
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.ERROR.getCode(), ResponseCode.ERROR.getMessage());
        }

    }

    /**
     *
     * 该方法用于将在展示模板中的资格判断有关数据置空
     * @param template
     */
    public void setTemplateQualifyJudgeNull(ExhibitionTemplate template){
        template.setJudgeType(null);
        template.setMktRightsCode(null);
        template.setMktRightsMonthType(null);
        template.setMktRightsMonthNum(null);
        template.setMktRightsStatus(null);
        template.setMktRightsRecordType(null);
        template.setMktActivityId(null);
        template.setMktPlanId(null);
    }

    @Override
    @Transactional(timeout= Constants.TRANSACTIONAL_TIMEOUT)
    public ResponseData delActivityDB(RequestData<GameActivityDto> requestData) {
        log.info("删除活动:{}",requestData);
        GameActivityDto gameActivityDto = JSON.parseObject(JSON.toJSONString(requestData.getContent()), GameActivityDto.class);
        try{
            if(gameActivityDto == null || gameActivityDto.getId() == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动id不能为空");
            }
            GameActivityDo gameActivityDo = gameActivityMapper.queryById(gameActivityDto.getId());
            if(gameActivityDo == null){
                //获取不到活动信息
                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_ACTIVITY_NOT_EXIST);
            }
            GameTemplateDo gameTemplateDo = gameTemplateMapper.queryByGameId(gameActivityDto.getId());
            if(gameActivityDo == null){
                //获取不到活动模板信息
                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_TEMPLATE_NOT_EXIST);
            }
            Date now = new Date();
            if (Constants.GAME_STATUS_UP.equals(gameActivityDo.getShelfStatus()) && now.before(gameActivityDo.getEndTime()) && now.after(gameActivityDo.getStartTime())) {
                //活动生效时间内为上架状态，无法修改
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动生效时间内为上架状态，无法修改");
            }


            //获取游戏模板奖品
            List<GameTemplateRewardDo> gameTemplateRewardDos = gameTemplateRewardMapper.queryByGameIdAndGameTemplateId(gameActivityDo.getId(), gameTemplateDo.getId());
            GameRewardPoolDo gameRewardPoolDoQuery = new GameRewardPoolDo();
            gameRewardPoolDoQuery.setGameId(gameActivityDo.getId());
            //gameRewardPoolDoQuery.setIsBackup("0");
            gameRewardPoolDoQuery.setIsDel("0");
            List<GameRewardPoolDo> gameRewardPoolDos = rewardPoolMapper.queryByRewardPool(gameRewardPoolDoQuery);
            if(gameRewardPoolDos != null && gameRewardPoolDos.size() > 0){
                for (GameRewardPoolDo gameRewardPoolDo : gameRewardPoolDos) {
                    List<GameRewardDo> gameRewardDos = rewardMapper.queryByGameIdAndPoolId(gameActivityDo.getId(), gameRewardPoolDo.getId());
                    if(gameRewardDos != null && gameRewardDos.size()>0){
                        for(int i = 0; i< gameRewardDos.size(); i++){
                            GameRewardDo gameRewardDo = gameRewardDos.get(i);
                            if(gameTemplateRewardDos != null && gameTemplateRewardDos.size() > 0){
                                GameTemplateRewardDo gameTemplateRewardDo = null;
                                List<GameRewardRelDo> gameRewardRelDos = null;
                                if ("tt".equals(gameActivityDo.getGameTemplateType())) {
                                    //根据rewardIndex找到gameTemplateRewardDo
                                    List<GameTemplateRewardDo> gameTemplateRewardDoList1 = gameTemplateRewardDos.stream().filter(gameTemplateReward -> gameRewardDo.getRewardIndex().equals(gameTemplateReward.getRewardIndex())).collect(Collectors.toList());
                                    if (CollectionUtils.isEmpty(gameTemplateRewardDoList1)) {
                                        //如果奖池内奖品与当前模板内的奖品不一致（脏数据
                                        gameRewardRelDos = rewardRelMapper.queryByRewardId(gameRewardDo.getId());
                                    }
                                    else {
                                        gameTemplateRewardDo = gameTemplateRewardDoList1.get(0); //有且只有一个
                                    }
                                }
                                else {
                                    gameTemplateRewardDo = gameTemplateRewardDos.get(i);
                                }
                                if (CollectionUtils.isEmpty(gameRewardRelDos)) {
                                    gameRewardRelDos = rewardRelMapper.queryByTemplateRewardIdAndRewardId(gameTemplateRewardDo.getId(), gameRewardDo.getId());
                                }

                                if(gameRewardRelDos != null && gameRewardRelDos.size()>0){
                                    for (GameRewardRelDo gameRewardRelDo : gameRewardRelDos) {
                                        //删除游戏模板奖励与实际奖品关系
                                        gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                        gameRewardRelDo.setIsDel("1");
                                        rewardRelMapper.delGameRewardRel(gameRewardRelDo);
                                    }
                                }
                            }


                            //删除奖池下的奖励信息
                            gameRewardDo.setIsDel("1");
                            gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            rewardMapper.delGameReward(gameRewardDo);
                            if (RedisUtil.get(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameRewardDo.getId()) != null) {
                                //删除redis库存
                                RedisUtil.del(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameRewardDo.getId());
                            }
                        }
                    }
                    if (Constants.WHITE_YES.equals(gameRewardPoolDo.getIsWhite())) {
                        //删除白名单权益
                        QualifyJudgeParamDo judgeParamDo = new QualifyJudgeParamDo();
                        judgeParamDo.setId(gameRewardPoolDo.getFocusJudgeId());
                        judgeParamDo.setIsDel("1");
                        judgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                        qualifyJudgeParamMapper.delQualifyJudgeParam(judgeParamDo);
                    }

                    //删除奖池
                    gameRewardPoolDo.setIsDel("1");
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                    rewardPoolMapper.delGameRewardPool(gameRewardPoolDo);
                }
            }

            //如果存在展示设置节点，删除展示模板中存在资格判断参数表中的数据
            String configContent = gameTemplateDo.getConfigContent();
            ConfigContentDto configContentDto = JSON.parseObject(configContent, ConfigContentDto.class);
            List<ExhibitionTemplate> templateList = configContentDto.getTemplates();
            if (templateList != null && templateList.size() > 0){
                for (ExhibitionTemplate template : templateList) {
                    if(StringUtil.isEmpty(template.getTemplatePriority())){
                        if(!"0".equals(template.getTemplatePriority())){
                            Long judgeId = template.getQualifyJudgeId();
                            QualifyJudgeParamDo qualifyJudgeParamDo = qualifyJudgeParamMapper.queryById(judgeId);

                            //删除展示模板中存在的资格判断有关数据
                            qualifyJudgeParamDo.setIsDel("1");
                            qualifyJudgeParamDo.setModifyOper(gameActivityDo.getModifyOper());
                            qualifyJudgeParamMapper.delQualifyJudgeParam(qualifyJudgeParamDo);
                        }
                    }

                }

            }

            //删除游戏模板奖品
            if(gameTemplateRewardDos != null && gameTemplateRewardDos.size() > 0){
                for (GameTemplateRewardDo gameTemplateRewardDo : gameTemplateRewardDos) {
                    gameTemplateRewardDo.setIsDel("1");
                    gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                    gameTemplateRewardMapper.delGameTemplateReward(gameTemplateRewardDo);
                }
            }

            if(gameActivityDo.getFocusJudgeId() != null){
                //删除资格判断
                QualifyJudgeParamDo qualifyJudgeParamDo = qualifyJudgeParamMapper.queryById(gameActivityDo.getFocusJudgeId());
                if(qualifyJudgeParamDo == null){
                    //资格判断信息不存在
                    return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_QUALIFY_JUDGE_PARAM_NOT_EXIST);
                }
                qualifyJudgeParamDo.setIsDel("1");
                qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                qualifyJudgeParamMapper.delQualifyJudgeParam(qualifyJudgeParamDo);
            }

            //删除游戏模板表
            gameTemplateDo.setIsDel("1");
            gameTemplateDo.setModifyOper(gameActivityDto.getModifyOper());
            gameTemplateMapper.delGameTemplate(gameTemplateDo);

            //删除游戏主表
            gameActivityDo.setIsDel("1");
            gameActivityDo.setModifyOper(gameActivityDto.getModifyOper());
            gameActivityMapper.delGameActivity(gameActivityDo);
            log.info("删除活动成功");
            return new ResponseData(ResponseCode.OK,gameActivityDo.getId());
        }catch (Exception e){
            log.error("删除活动异常",e);
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.ERROR.getCode(), ResponseCode.ERROR.getMessage());
        }
    }

    @Override
    @Transactional(timeout= Constants.TRANSACTIONAL_TIMEOUT)
    public ResponseData updateActivityStatusDB(RequestData<GameActivityDto> requestData) {
        log.info("修改活动状态:{}",requestData);
        GameActivityDto gameActivityDto = JSON.parseObject(JSON.toJSONString(requestData.getContent()), GameActivityDto.class);
        try{
            if(gameActivityDto == null || gameActivityDto.getId() == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动id不能为空");
            }
            if (StringUtils.isBlank(gameActivityDto.getShelfStatus())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动状态不能为空");
            }
            GameActivityDo gameActivityDo = gameActivityMapper.queryById(gameActivityDto.getId());
            if(gameActivityDo == null){
                //获取不到活动信息
                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_ACTIVITY_NOT_EXIST);
            }
            //上架校验活动是否在有效期间
            Date now = new Date();
            if (Constants.GAME_STATUS_UP.equals(gameActivityDto.getShelfStatus()) && now.after(gameActivityDo.getEndTime())) {
                //如果非活动生效期间内，无法上架
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动已结束，无法上线");
            }

            if ("tt".equals(gameActivityDo.getGameTemplateType()) && "1".equals(gameActivityDto.getShelfStatus())) { //转盘上架校验redis库存值
                List<GameRewardDo> gameRewardDos = rewardMapper.queryByGameId(gameActivityDo.getId());
                if (gameRewardDos.stream().anyMatch(reward -> StringUtils.isBlank(RedisUtil.get(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + reward.getId())))) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"活动库存缓存值为null");
                }
            }
/*
            if (gameActivityDo.getGameStatus().equals(gameActivityDto.getGameStatus())) {
                return new ResponseData(ResponseCode.OK.getCode(), ResponseCode.OK.getMessage());
            }
*/

            // 下架其他正在上架的版本活动
            if (Constants.GAME_STATUS_UP.equals(gameActivityDto.getShelfStatus())){
                // 查询当前活动正在上架的版本
                GameActivityDo upStatusGame = gameActivityMapper.queryUpActivityById(gameActivityDo.getId());
                if (upStatusGame!=null){
                    //下架添加一条审核记录
                    AuditTaskDo auditTaskDo = new AuditTaskDo();
                    auditTaskDo.setAuditOper(gameActivityDto.getModifyOper());
                    auditTaskDo.setSubmitOper(gameActivityDto.getModifyOper());
                    auditTaskDo.setAuditStatus(Constants.RIGHTS_AUDIT_STATUS_PASS);
                    auditTaskDo.setAuditType(Integer.valueOf(gameActivityDto.getShelfStatus()));
                    auditTaskDo.setContentType(Constants.RIGHTS_CONTENT_TYPE_GAME_CENTER);
                    auditTaskDo.setAuditOperName(gameActivityDto.getModifyOper());
                    auditTaskDo.setSubmitOperName(gameActivityDto.getModifyOper());
                    auditTaskDo.setContentId(upStatusGame.getId());
                    auditTaskDo.setTaskName(upStatusGame.getGameName());
                    auditTaskDo.setGmtAudit(LocalDateTime.now());
                    auditTaskDo.setGmtSubmit(LocalDateTime.now());
                    auditTaskDo.setAuditComment("上架其他版本活动");
                    auditTaskService.autoTask(auditTaskDo);
                    // 下架其他正在上架的版本活动
                    upStatusGame.setShelfStatus(Constants.GAME_STATUS_DOWN);
                    upStatusGame.setModifyOper(gameActivityDto.getModifyOper());
                    gameActivityMapper.updateGameActivityStatus(upStatusGame);
                }
            }


            gameActivityDo.setShelfStatus(gameActivityDto.getShelfStatus());
            gameActivityDo.setModifyOper(gameActivityDto.getModifyOper());
            gameActivityMapper.updateGameActivityStatus(gameActivityDo);

            log.info("修改活动状态成功");
            return new ResponseData(ResponseCode.OK,gameActivityDo.getId());
        }catch (Exception e){
            log.error("修改活动状态异常",e);
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.ERROR.getCode(), ResponseCode.ERROR.getMessage());
        }
    }


    public void delBackUpPool(GameActivityDto gameActivityDto, GameActivityDo gameActivityDo, List<GameRewardPoolDo> backUpPools) {
        if (backUpPools != null && backUpPools.size() > 0) {
            for (GameRewardPoolDo gameRewardPoolDo : backUpPools) {
                List<GameRewardDo> gameRewardDos = rewardMapper.queryByGameIdAndPoolId(gameActivityDo.getId(), gameRewardPoolDo.getId());
                if (gameRewardDos != null && gameRewardDos.size() > 0) {
                    for (GameRewardDo gameRewardDo : gameRewardDos) {
                        //删除奖励
                        gameRewardDo.setIsDel("1");
                        gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                        rewardMapper.delGameReward(gameRewardDo);
                        //删除游戏模板奖励与实际奖品关系
                        GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                        gameRewardRelDo.setIsDel("1");
                        gameRewardRelDo.setRewardId(gameRewardDo.getId());
                        gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                        rewardRelMapper.delByRewardId(gameRewardRelDo);
                    }
                }
                //删除奖池
                gameRewardPoolDo.setIsDel("1");
                gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                rewardPoolMapper.delGameRewardPool(gameRewardPoolDo);
            }
        }
    }

    public List<GameRewardPoolDo> poolOperate(GameActivityDto gameActivityDto, GameActivityDo gameActivityDo, List<GameTemplateRewardDo> gameTemplateRewardDos, ArrayList<GameTemplateRewardDo> gameTemplateRewardDoList, ArrayList<GameTemplateRewardDo> gameTemplateRewardDoUpdateList, List<GameRewardPoolDto> poolList, List<GameRewardPoolDo> mainPool) throws Exception {
        List<GameRewardPoolDto> insertPool = new ArrayList<>();
        List<GameRewardPoolDto> deletePool = new ArrayList<>();
        List<GameRewardPoolDto> updatePool = new ArrayList<>();
        List<GameRewardPoolDo> pools = new ArrayList<>();//新增或修改奖池，存至pools
        if(poolList != null && poolList.size() > 0){
            for (GameRewardPoolDto pool :poolList) {
                if(pool.getId() == null){
                    insertPool.add(pool);
                }else{
                    if("1".equals(pool.getIsDel())){
                        deletePool.add(pool);
                    }else{
                        updatePool.add(pool);
                    }
                }
            }
        }


        try {
            //删除奖池
            if(deletePool.size() > 0){
                for (GameRewardPoolDto poolDto : deletePool) {
                    List<GameRewardDto> rewardList = poolDto.getRewardList();
                    if(rewardList != null && rewardList.size() > 0){
                        for(int i = 0; i< rewardList.size(); i++){
                            GameRewardDto gameRewardDto = rewardList.get(i);
                            if(gameTemplateRewardDos != null && gameTemplateRewardDos.size() > 0) {
                                GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDos.get(i);
                                List<GameRewardRelDo> gameRewardRelDos = rewardRelMapper.queryByTemplateRewardIdAndRewardId(gameTemplateRewardDo.getId(), gameRewardDto.getId());
                                if (gameRewardRelDos != null && gameRewardRelDos.size() > 0) {
                                    for (GameRewardRelDo gameRewardRelDo : gameRewardRelDos) {
                                        //删除游戏模板奖励与实际奖品关系
                                        gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                        gameRewardRelDo.setIsDel("1");
                                        rewardRelMapper.delGameRewardRel(gameRewardRelDo);
                                    }
                                }
                            }

                            //删除奖池下的奖励信息
                            GameRewardDo gameRewardDo = new GameRewardDo();
                            gameRewardDo.setId(gameRewardDto.getId());
                            gameRewardDo.setIsDel("1");
                            gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            rewardMapper.delGameReward(gameRewardDo);
                        }
                    }

                    //删除奖池
                    GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                    gameRewardPoolDo.setId(poolDto.getId());
                    gameRewardPoolDo.setIsDel("1");
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                    rewardPoolMapper.delGameRewardPool(gameRewardPoolDo);
                }
            }

            //新增奖池
            if(insertPool.size() > 0){
                for (GameRewardPoolDto poolDto : insertPool) {
                    //插入奖池信息
                    GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                    gameRewardPoolDo.setGameId(gameActivityDto.getId());
                    gameRewardPoolDo.setIsBackup(poolDto.getIsBackup());
                    if("1".equals(poolDto.getIsBackup())){
                        for (GameRewardPoolDo tmp : mainPool) {
                            if(poolDto.getMktRightsNum().equals(tmp.getMktRightsNum())){
                                gameRewardPoolDo.setMainPoolId(tmp.getId());
                            }
                        }
                    }
                    gameRewardPoolDo.setBackupPoolType(poolDto.getBackupPoolType());
                    gameRewardPoolDo.setPoolName(poolDto.getPoolName());
                    gameRewardPoolDo.setMktRightsNum(poolDto.getMktRightsNum());
                    gameRewardPoolDo.setCreateOper(gameActivityDto.getCreateOper());
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                    gameRewardPoolDo.setDeptId(gameActivityDto.getDeptId());

                    rewardPoolMapper.insertGameRewardPool(gameRewardPoolDo);
                    pools.add(gameRewardPoolDo);
                    List<GameRewardDto> rewardList = poolDto.getRewardList();
                    if(rewardList != null && rewardList.size() > 0){
                        //插入奖励信息
                        for (int i = 0; i< rewardList.size(); i++) {
                            GameRewardDto gameRewardDto = rewardList.get(i);
                            GameRewardDo gameRewardDo = new GameRewardDo();
                            gameRewardDo.setGameId(gameActivityDo.getId());
                            gameRewardDo.setPoolId(gameRewardPoolDo.getId());
                            gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                            gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                            BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                            BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                            gameRewardDo.setRate(rate.intValue());
                            gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                            gameRewardDo.setAmount2(gameRewardDto.getAmount2());
                            gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                            gameRewardDo.setIntegral2(gameRewardDto.getIntegral2());
                            gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                            gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                            gameRewardDo.setElmeCaseNo(gameRewardDto.getElmeCaseNo());
                            gameRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                            gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            gameRewardDo.setDeptId(gameActivityDto.getDeptId());

                            gameRewardDo.setRedoubleEnable(gameRewardDto.getRedoubleEnable());
                            if("1".equals(gameRewardDto.getRedoubleEnable())){
                                BigDecimal bigRedoubleRate = new BigDecimal(gameRewardDto.getRedoubleRate());
                                BigDecimal redoubleRate = bigRedoubleRate.multiply(new BigDecimal(10000));
                                gameRewardDo.setRedoubleRate(redoubleRate.intValue());
                            }
                            gameRewardDo.setRedoubleOptions(gameRewardDto.getRedoubleOptions());
                            gameRewardDo.setRedoubleTimes(gameRewardDto.getRedoubleTimes());
                            gameRewardDo.setRedoubleMinPeriod(gameRewardDto.getRedoubleMinPeriod());
                            gameRewardDo.setRedoubleMaxPeriod(gameRewardDto.getRedoubleMaxPeriod());
                            gameRewardDo.setRedoubleLabelText(gameRewardDto.getRedoubleLabelText());
                            gameRewardDo.setRedoubleLabelColor(gameRewardDto.getRedoubleLabelColor());


                            rewardMapper.insertGameReward(gameRewardDo);


                            if(gameTemplateRewardDoList.size() > 0) {
                                GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList.get(i);
                                //插入游戏模板奖励与实际奖品关系
                                GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                                gameRewardRelDo.setTemplateRewardId(gameTemplateRewardDo.getId());
                                gameRewardRelDo.setRewardId(gameRewardDo.getId());
                                gameRewardRelDo.setCreateOper(gameActivityDto.getCreateOper());
                                gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                gameRewardRelDo.setDeptId(gameActivityDto.getDeptId());
                                rewardRelMapper.insertGameRewardRel(gameRewardRelDo);
                            }
                        }
                    }
                }
            }

            //修改奖池
            if(updatePool.size() > 0){
                for (GameRewardPoolDto poolDto : updatePool) {
                    GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                    gameRewardPoolDo.setId(poolDto.getId());
                    gameRewardPoolDo.setIsBackup(poolDto.getIsBackup());
                    gameRewardPoolDo.setBackupPoolType(poolDto.getBackupPoolType());
                    gameRewardPoolDo.setPoolName(poolDto.getPoolName());
                    gameRewardPoolDo.setMktRightsNum(poolDto.getMktRightsNum());
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());


                    rewardPoolMapper.updateGameRewardPool(gameRewardPoolDo);
                    pools.add(gameRewardPoolDo);
                    List<GameRewardDto> rewardList = poolDto.getRewardList();
                    if(rewardList != null && rewardList.size() >0){
                        for (int i = 0; i< rewardList.size(); i++) {
                            GameRewardDto gameRewardDto = rewardList.get(i);

                            if(gameRewardDto.getId() == null){
                                //新增
                                GameRewardDo gameRewardDo = new GameRewardDo();
                                gameRewardDo.setGameId(gameActivityDo.getId());
                                gameRewardDo.setPoolId(gameRewardPoolDo.getId());
                                gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                                gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                                BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                                BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                                gameRewardDo.setRate(rate.intValue());
                                gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                                gameRewardDo.setAmount2(gameRewardDto.getAmount2());
                                gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                                gameRewardDo.setIntegral2(gameRewardDto.getIntegral2());
                                gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                                gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                                gameRewardDo.setElmeCaseNo(gameRewardDto.getElmeCaseNo());
                                gameRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                                gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                                gameRewardDo.setDeptId(gameActivityDto.getDeptId());

                                gameRewardDo.setRedoubleEnable(gameRewardDto.getRedoubleEnable());

                                if("1".equals(gameRewardDto.getRedoubleEnable())){
                                    BigDecimal bigRedoubleRate = new BigDecimal(gameRewardDto.getRedoubleRate());
                                    BigDecimal redoubleRate = bigRedoubleRate.multiply(new BigDecimal(10000));
                                    gameRewardDo.setRedoubleRate(redoubleRate.intValue());
                                }
                                gameRewardDo.setRedoubleOptions(gameRewardDto.getRedoubleOptions());
                                gameRewardDo.setRedoubleTimes(gameRewardDto.getRedoubleTimes());
                                gameRewardDo.setRedoubleMinPeriod(gameRewardDto.getRedoubleMinPeriod());
                                gameRewardDo.setRedoubleMaxPeriod(gameRewardDto.getRedoubleMaxPeriod());
                                gameRewardDo.setRedoubleLabelText(gameRewardDto.getRedoubleLabelText());
                                gameRewardDo.setRedoubleLabelColor(gameRewardDto.getRedoubleLabelColor());


                                rewardMapper.insertGameReward(gameRewardDo);
                                if(gameTemplateRewardDoUpdateList.size() > 0) {
                                    GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoUpdateList.get(i);
                                    //插入游戏模板奖励与实际奖品关系
                                    GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                                    gameRewardRelDo.setTemplateRewardId(gameTemplateRewardDo.getId());
                                    gameRewardRelDo.setRewardId(gameRewardDo.getId());
                                    gameRewardRelDo.setCreateOper(gameActivityDto.getCreateOper());
                                    gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                    gameRewardRelDo.setDeptId(gameActivityDto.getDeptId());
                                    rewardRelMapper.insertGameRewardRel(gameRewardRelDo);
                                }
                            }else{
                                if("1".equals(gameRewardDto.getIsDel())){
                                    //删除
                                    if(gameTemplateRewardDoUpdateList.size() > 0) {
                                        GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoUpdateList.get(i);
                                        List<GameRewardRelDo> gameRewardRelDos = rewardRelMapper.queryByTemplateRewardIdAndRewardId(gameTemplateRewardDo.getId(), gameRewardDto.getId());
                                        if (gameRewardRelDos != null && gameRewardRelDos.size() > 0) {
                                            for (GameRewardRelDo gameRewardRelDo : gameRewardRelDos) {
                                                //删除游戏模板奖励与实际奖品关系
                                                gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                                gameRewardRelDo.setIsDel("1");
                                                rewardRelMapper.delGameRewardRel(gameRewardRelDo);
                                            }
                                        }
                                    }
                                    //删除奖池下的奖励信息
                                    GameRewardDo gameRewardDo = new GameRewardDo();
                                    gameRewardDo.setId(gameRewardDto.getId());
                                    gameRewardDo.setIsDel("1");
                                    gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                                    rewardMapper.delGameReward(gameRewardDo);
                                }else{
                                    //修改

                                    GameRewardDo gameRewardDo = new GameRewardDo();
                                    gameRewardDo.setId(gameRewardDto.getId());
                                    gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                                    gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                                    BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                                    BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                                    gameRewardDo.setRate(rate.intValue());
                                    gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                                    gameRewardDo.setAmount2(gameRewardDto.getAmount2());
                                    gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                                    gameRewardDo.setIntegral2(gameRewardDto.getIntegral2());
                                    gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                                    gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                                    gameRewardDo.setElmeCaseNo(gameRewardDto.getElmeCaseNo());
                                    gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());

                                    gameRewardDo.setRedoubleEnable(gameRewardDto.getRedoubleEnable());
                                    if("1".equals(gameRewardDto.getRedoubleEnable())){
                                        BigDecimal bigRedoubleRate = new BigDecimal(gameRewardDto.getRedoubleRate());
                                        BigDecimal redoubleRate = bigRedoubleRate.multiply(new BigDecimal(10000));
                                        gameRewardDo.setRedoubleRate(redoubleRate.intValue());
                                    }
                                    gameRewardDo.setRedoubleOptions(gameRewardDto.getRedoubleOptions());
                                    gameRewardDo.setRedoubleTimes(gameRewardDto.getRedoubleTimes());
                                    gameRewardDo.setRedoubleMinPeriod(gameRewardDto.getRedoubleMinPeriod());
                                    gameRewardDo.setRedoubleMaxPeriod(gameRewardDto.getRedoubleMaxPeriod());
                                    gameRewardDo.setRedoubleLabelText(gameRewardDto.getRedoubleLabelText());
                                    gameRewardDo.setRedoubleLabelColor(gameRewardDto.getRedoubleLabelColor());
                                    rewardMapper.updateGameReward(gameRewardDo);
                                }
                            }
                        }
                    }
                }
            }
        }
        catch (IllegalArgumentException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(e.getMessage());
        }
        catch (Exception e) {
            log.error("奖池修改异常", e.getMessage(), e);
            throw new Exception(e);
        }

        return pools;
    }

    /**
     * 校验活动数据
     */
    private ResponseData checkData(GameActivityDto gameActivityDto){
        if(StringUtil.isEmpty(gameActivityDto.getGameTemplateType())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"游戏模板类型不能为空");
        }

        if(StringUtil.isEmpty(gameActivityDto.getGameName())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"游戏名称不能为空");
        }

        if(gameActivityDto.getStartTime() == null){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"开始时间不能为空");
        }

        if(gameActivityDto.getEndTime() == null){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"结束时间不能为空");
        }

        if(gameActivityDto.getConfigContent() == null){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"配置内容不能为空");
        }

/*        if (StringUtils.isBlank(gameActivityDto.getGameStatus())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(),"配置内容不能为空");
        }*/

        if(gameActivityDto.getEndTime().before(gameActivityDto.getStartTime())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"开始时间不能大于结束时间");
        }
        ConfigContentDto configContent = gameActivityDto.getConfigContent();
        if(!"hb3".equals(gameActivityDto.getGameTemplateType()) && StringUtil.isEmpty(configContent.getMainView())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"主觉视图不能为空");
        }
        if ("hb3".equals(gameActivityDto.getGameTemplateType())){
            List<ExhibitionTemplate> templates = configContent.getTemplates();
            for (ExhibitionTemplate exhibitionTemplate: templates  ) {

                if(StringUtil.isEmpty(exhibitionTemplate.getTemplatePriority())){
                    throw new InvalidParameterException("模板展示优先级不能为空");
                }
                if (!"0".equals(exhibitionTemplate.getTemplatePriority())){

                    if (StringUtil.isEmpty(exhibitionTemplate.getMainView())){
                        throw new InvalidParameterException("主视图不能为空");
                    }
                    if (StringUtil.isEmpty(exhibitionTemplate.getJudgeType())){
                        throw new InvalidParameterException("模板可见不能为空");
                    }else {
                        //营管权益资格判断
                        if ("5".equals(exhibitionTemplate.getJudgeType())){
                            if (StringUtil.isEmpty(exhibitionTemplate.getMktRightsCode())
                                    || StringUtil.isEmpty(exhibitionTemplate.getMktRightsMonthType())
                                    || StringUtil.isEmpty(exhibitionTemplate.getMktRightsStatus())
                                    || StringUtil.isEmpty(exhibitionTemplate.getMktRightsRecordType())
                            ){
                                throw new InvalidParameterException("营管权益资格判断条件不能为空");
                            }
                            //营管权益资格判断
                        }if ("2".equals(exhibitionTemplate.getJudgeType())){
                            if (StringUtil.isEmpty(exhibitionTemplate.getMktActivityId())
                                    || StringUtil.isEmpty(exhibitionTemplate.getMktPlanId())
                            ){
                                throw new InvalidParameterException("营管达标资格判断条件不能为空");
                            }
                        }
                    }

                }
            }
        }

        if("box".equals(gameActivityDto.getGameTemplateType())){
            if(configContent.getRewardInfoList() == null || configContent.getRewardInfoList() .size() < 3){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"饿了么盲盒模板盲盒图片最少需配置3组");
            }
        }

        if(!StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) && !StringUtil.isEmpty(gameActivityDto.getMktNameId())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"分众可见的营管权益编码和名单id只能填一个");
        }

        if(checkAmount(gameActivityDto.getAmountTotalLimit())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"总限额最大值为99999999.99");
        }
        if(checkAmount(gameActivityDto.getAmountAnnuallyLimit())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"年限额最大值为99999999.99");
        }
        if(checkAmount(gameActivityDto.getAmountMonthlyLimit())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"月度限额最大值为99999999.99");
        }
        if(checkAmount(gameActivityDto.getAmountPersonalTotalLimit())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"个人总限额最大值为99999999.99");
        }
        if(checkAmount(gameActivityDto.getAmountPersonalMonthlyLimit())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"个人月度限额最大值为99999999.99");
        }
        if(!StringUtil.isEmpty(gameActivityDto.getIsEnableHighReward()) && "1".equals(gameActivityDto.getIsEnableHighReward())){
            //开启高奖励限制
            Integer highRewardDays = gameActivityDto.getHighRewardDays();
            Integer highRewardTimes = gameActivityDto.getHighRewardTimes();
            BigDecimal highRewardAmount = gameActivityDto.getHighRewardAmount();
            if(highRewardDays == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"高奖励限制天数不能为空");
            }
            if(highRewardTimes == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"高奖励限制次数不能为空");
            }
            if(highRewardAmount == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"高奖励限制金额不能为空");
            }
            if(highRewardDays < 1 || highRewardDays > 9999){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"高奖励限制天数范围为1-9999");
            }
            if(highRewardTimes < 1 || highRewardTimes > 99){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"高奖励限制次数范围为1-99");
            }
            if(highRewardAmount.compareTo(BigDecimal.ZERO) <= 0
                    || !isNumber(highRewardAmount.toString())
                    || highRewardAmount.compareTo(new BigDecimal("9999.99")) > 0){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"高奖励限制金额范围为0.01-9999.99,小数位最多为两位");
            }
        }
        //校验奖池权益编号
        List<GameRewardPoolDto> poolList = gameActivityDto.getPoolList();
        if(poolList == null || poolList.size() == 0){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池列表不能为空");
        }
        List<GameRewardPoolDto> mainPools = new ArrayList<>();//主奖池
        for (GameRewardPoolDto gameRewardPoolDto : poolList) {
            if(!"1".equals(gameRewardPoolDto.getIsDel())){
                //主奖池
                this.getPools(mainPools, gameRewardPoolDto);
            }
        }
        //奖池校验
        ResponseData res = this.checkRewardPool(mainPools,gameActivityDto);
        if (res != null){
            return res;
        }
        //备用奖池
        List<GameRewardPoolDto> amountTotalBkupPools = new ArrayList<>();//刷卡金奖励总限额备用奖池
        List<GameRewardPoolDto> amountAnnuallyBkupPools = new ArrayList<>();//刷卡金奖励年度限额备用奖池
        List<GameRewardPoolDto> amountMonthlyBkupPools = new ArrayList<>();//刷卡金奖励月度限额备用奖池
        List<GameRewardPoolDto> amountPersonalTotalBkupPools = new ArrayList<>();//刷卡金个人总限额备用奖池
        List<GameRewardPoolDto> amountPersonalMonthlyBkupPools = new ArrayList<>();//刷卡金个人月度限额备用奖池
        List<GameRewardPoolDto> amountPersonalYearlyBkupPools = new ArrayList<>();//刷卡金个人年度限额备用奖池
        BackupPoolListDto backupPoolList = gameActivityDto.getBackupPoolList();
        if("1".equals(gameActivityDto.getIsEnableBkupPool())){
            //校验备用奖池是否存在
            if(backupPoolList == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(), "启用备用奖池但是备用奖池信息不存在");
            }

            List<GameRewardPoolDto> amountTotalPools = backupPoolList.getAmountTotalPools();//刷卡金奖励总限额备用奖池
            List<GameRewardPoolDto> amountAnnuallyPools = backupPoolList.getAmountAnnuallyPools();//刷卡金奖励年度限额备用奖池
            List<GameRewardPoolDto> amountMonthlyPools = backupPoolList.getAmountMonthlyPools();//刷卡金奖励月度限额备用奖池
            List<GameRewardPoolDto> amountPersonalTotalPools = backupPoolList.getAmountPersonalTotalPools();//刷卡金个人总限额备用奖池
            List<GameRewardPoolDto> amountPersonalMonthlyPools = backupPoolList.getAmountPersonalMonthlyPools();//刷卡金个人月度限额备用奖池
            List<GameRewardPoolDto> amountPersonalYearlyPools = backupPoolList.getAmountPersonalYearlyPools();//刷卡金个人年度限额备用奖池

            if((amountTotalPools == null || amountTotalPools.size() == 0)
                    && (amountAnnuallyPools == null || amountAnnuallyPools.size() == 0)
                    && (amountMonthlyPools == null || amountMonthlyPools.size() == 0)
                    && (amountPersonalTotalPools == null || amountPersonalTotalPools.size() == 0)
                    && (amountPersonalMonthlyPools == null || amountPersonalMonthlyPools.size() == 0)
                    && (amountPersonalYearlyPools == null || amountPersonalYearlyPools.size() == 0)){
                return new ResponseData(ResponseCode.CODE_403.getCode(), "启用备用奖池但是备用奖池信息不存在");
            }

            if(amountTotalPools != null && amountTotalPools.size() > 0){
                for (GameRewardPoolDto gameRewardPoolDto:amountTotalPools) {
                    if(!"1".equals(gameRewardPoolDto.getIsDel())){
                        this.getPools(amountTotalBkupPools, gameRewardPoolDto);
                    }
                }
            }
            if(amountAnnuallyPools != null && amountAnnuallyPools.size() > 0){
                for (GameRewardPoolDto gameRewardPoolDto:amountAnnuallyPools) {
                    if(!"1".equals(gameRewardPoolDto.getIsDel())){
                        this.getPools(amountAnnuallyBkupPools, gameRewardPoolDto);
                    }
                }
            }
            if(amountMonthlyPools != null && amountMonthlyPools.size() > 0){
                for (GameRewardPoolDto gameRewardPoolDto:amountMonthlyPools) {
                    if(!"1".equals(gameRewardPoolDto.getIsDel())){
                        this.getPools(amountMonthlyBkupPools, gameRewardPoolDto);
                    }
                }
            }
            if(amountPersonalTotalPools != null && amountPersonalTotalPools.size() > 0){
                for (GameRewardPoolDto gameRewardPoolDto:amountPersonalTotalPools) {
                    if(!"1".equals(gameRewardPoolDto.getIsDel())){
                        this.getPools(amountPersonalTotalBkupPools, gameRewardPoolDto);
                    }
                }
            }
            if(amountPersonalMonthlyPools != null && amountPersonalMonthlyPools.size() > 0){
                for (GameRewardPoolDto gameRewardPoolDto:amountPersonalMonthlyPools) {
                    if(!"1".equals(gameRewardPoolDto.getIsDel())){
                        this.getPools(amountPersonalMonthlyBkupPools, gameRewardPoolDto);
                    }
                }
            }
            if(amountPersonalYearlyPools != null && amountPersonalYearlyPools.size() > 0){
                for (GameRewardPoolDto gameRewardPoolDto:amountPersonalYearlyPools) {
                    if(!"1".equals(gameRewardPoolDto.getIsDel())){
                        this.getPools(amountPersonalYearlyBkupPools, gameRewardPoolDto);
                    }
                }
            }
            if(amountTotalBkupPools.size() > 0){
                ResponseData backUpPoolsRes = this.checkRewardPool(amountTotalBkupPools,null);
                if (backUpPoolsRes != null){
                    return backUpPoolsRes;
                }
            }
            if(amountAnnuallyBkupPools.size() > 0){
                ResponseData backUpPoolsRes = this.checkRewardPool(amountAnnuallyBkupPools,null);
                if (backUpPoolsRes != null){
                    return backUpPoolsRes;
                }
            }
            if(amountMonthlyBkupPools.size() > 0){
                ResponseData backUpPoolsRes = this.checkRewardPool(amountMonthlyBkupPools,null);
                if (backUpPoolsRes != null){
                    return backUpPoolsRes;
                }
            }
            if(amountPersonalTotalBkupPools.size() > 0){
                ResponseData backUpPoolsRes = this.checkRewardPool(amountPersonalTotalBkupPools,null);
                if (backUpPoolsRes != null){
                    return backUpPoolsRes;
                }
            }
            if(amountPersonalMonthlyBkupPools.size() > 0){
                ResponseData backUpPoolsRes = this.checkRewardPool(amountPersonalMonthlyBkupPools,null);
                if (backUpPoolsRes != null){
                    return backUpPoolsRes;
                }
            }
            if(amountPersonalYearlyBkupPools.size() > 0){
                ResponseData backUpPoolsRes = this.checkRewardPool(amountPersonalYearlyBkupPools,null);
                if (backUpPoolsRes != null){
                    return backUpPoolsRes;
                }
            }
        }

        //校验游戏模板中奖励信息数量与每个奖池下奖励数量是否一致
        if(configContent.getRewardInfoList() != null && configContent.getRewardInfoList().size() > 0){
            ArrayList<GameRewardInfoDto> gameRewardInfoDtos = new ArrayList<>();
            for (GameRewardInfoDto gameRewardInfoDto :configContent.getRewardInfoList()) {
                if(!"1".equals(gameRewardInfoDto.getIsDel())){
                    gameRewardInfoDtos.add(gameRewardInfoDto);
                }
            }
            if(gameRewardInfoDtos.size() > 0){
                //主奖池
                for (GameRewardPoolDto gameRewardPoolDto : mainPools) {
                    if(gameRewardInfoDtos.size() != gameRewardPoolDto.getRewardList().size()){
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板中奖励信息数量与主奖池下奖励数量不一致");
                    }
                }
                //刷卡金奖励总限额备用奖池
                if(amountTotalBkupPools.size() > 0){
                    for (GameRewardPoolDto gameRewardPoolDto : amountTotalBkupPools) {
                        if(gameRewardInfoDtos.size() != gameRewardPoolDto.getRewardList().size()){
                            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板中奖励信息数量与刷卡金奖励总限额备用奖池下奖励数量不一致");
                        }
                    }
                }
                //刷卡金奖励年度限额备用奖池
                if(amountAnnuallyBkupPools.size() > 0){
                    for (GameRewardPoolDto gameRewardPoolDto : amountAnnuallyBkupPools) {
                        if(gameRewardInfoDtos.size() != gameRewardPoolDto.getRewardList().size()){
                            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板中奖励信息数量与刷卡金奖励年度限额备用奖池下奖励数量不一致");
                        }
                    }
                }
                //刷卡金奖励月度限额备用奖池
                if(amountMonthlyBkupPools.size() > 0){
                    for (GameRewardPoolDto gameRewardPoolDto : amountMonthlyBkupPools) {
                        if(gameRewardInfoDtos.size() != gameRewardPoolDto.getRewardList().size()){
                            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板中奖励信息数量与刷卡金奖励月度限额备用奖池奖励数量不一致");
                        }
                    }
                }
                //刷卡金个人总限额备用奖池
                if(amountPersonalTotalBkupPools.size() > 0){
                    for (GameRewardPoolDto gameRewardPoolDto : amountPersonalTotalBkupPools) {
                        if(gameRewardInfoDtos.size() != gameRewardPoolDto.getRewardList().size()){
                            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板中奖励信息数量与刷卡金个人总限额备用奖池下奖励数量不一致");
                        }
                    }
                }
                //刷卡金个人月度限额备用奖池
                if(amountPersonalMonthlyBkupPools.size() > 0){
                    for (GameRewardPoolDto gameRewardPoolDto : amountPersonalMonthlyBkupPools) {
                        if(gameRewardInfoDtos.size() != gameRewardPoolDto.getRewardList().size()){
                            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板中奖励信息数量与刷卡金个人月度限额备用奖池奖励数量不一致");
                        }
                    }
                }
            }
        }
        return new ResponseData(ResponseCode.OK.getCode(), ResponseCode.OK.getMessage());
    }

    /**
     *  保存奖励池信息
     * @param gameActivityDto 请求数据
     * @param gameActivityDo 数据库中数据
     * @param configContentDto 请求中ConfigContentDto数据
     * @param rewardInfos       奖品信息列表
     * @param gameTemplateRewardDos 游戏模板奖品列表
     * @param poolList      奖励池列表updateActivity
     * @return 奖池信息
     */
    public List<GameRewardPoolDo> savePools(GameActivityDto gameActivityDto, GameActivityDo gameActivityDo, ConfigContentDto configContentDto, List<GameRewardInfoDto> rewardInfos, ArrayList<GameTemplateRewardDo> gameTemplateRewardDos, List<GameRewardPoolDto> poolList) {
        ArrayList<GameRewardPoolDo> result = new ArrayList<>();
        if(poolList != null && poolList.size() >0){
            for (GameRewardPoolDto gameRewardPoolDto :poolList) {
                //插入奖池信息
                GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                gameRewardPoolDo.setGameId(gameActivityDo.getId());
                gameRewardPoolDo.setIsBackup(gameRewardPoolDto.getIsBackup());
                gameRewardPoolDo.setMainPoolId(gameRewardPoolDto.getMainPoolId());
                gameRewardPoolDo.setBackupPoolType(gameRewardPoolDto.getBackupPoolType());
                gameRewardPoolDo.setPoolName(gameRewardPoolDto.getPoolName());
                gameRewardPoolDo.setMktRightsNum(gameRewardPoolDto.getMktRightsNum());
                gameRewardPoolDo.setCreateOper(gameActivityDto.getCreateOper());
                gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                gameRewardPoolDo.setDeptId(gameActivityDto.getDeptId());
                rewardPoolMapper.insertGameRewardPool(gameRewardPoolDo);


                result.add(gameRewardPoolDo);
                List<GameRewardDto> rewardList = gameRewardPoolDto.getRewardList();
                if(rewardList != null && rewardList.size() >0){
                    if(rewardInfos != null && rewardInfos.size() > 0){
                        if(rewardList.size() != gameTemplateRewardDos.size()){
//                            return null;
                            throw new InvalidParameterException("奖池奖品数量与游戏模板奖励数量不一致");
                        }
                    }
                    //插入奖励信息
                    for (int i = 0; i< rewardList.size(); i++) {
                        GameRewardDto gameRewardDto = rewardList.get(i);
                        GameRewardDo gameRewardDo = new GameRewardDo();
                        gameRewardDo.setGameId(gameActivityDo.getId());
                        gameRewardDo.setPoolId(gameRewardPoolDo.getId());
                        gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                        gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                        BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                        BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                        gameRewardDo.setRate(rate.intValue());
                        gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                        gameRewardDo.setAmount2(gameRewardDto.getAmount2());
                        gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                        gameRewardDo.setIntegral2(gameRewardDto.getIntegral2());
                        gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                        gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                        gameRewardDo.setElmeCaseNo(gameRewardDto.getElmeCaseNo());
                        gameRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                        gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                        gameRewardDo.setDeptId(gameActivityDto.getDeptId());

                        gameRewardDo.setRedoubleEnable(gameRewardDto.getRedoubleEnable());

                        if("1".equals(gameRewardDto.getRedoubleEnable())){
                            BigDecimal bigRedoubleRate = new BigDecimal(gameRewardDto.getRedoubleRate());
                            BigDecimal redoubleRate = bigRedoubleRate.multiply(new BigDecimal(10000));
                            gameRewardDo.setRedoubleRate(redoubleRate.intValue());
                        }
                        gameRewardDo.setRedoubleOptions(gameRewardDto.getRedoubleOptions());
                        gameRewardDo.setRedoubleTimes(gameRewardDto.getRedoubleTimes());
                        gameRewardDo.setRedoubleMinPeriod(gameRewardDto.getRedoubleMinPeriod());
                        gameRewardDo.setRedoubleMaxPeriod(gameRewardDto.getRedoubleMaxPeriod());
                        gameRewardDo.setRedoubleLabelText(gameRewardDto.getRedoubleLabelText());
                        gameRewardDo.setRedoubleLabelColor(gameRewardDto.getRedoubleLabelColor());
                        rewardMapper.insertGameReward(gameRewardDo);

                        if(configContentDto.getRewardInfoList() != null && configContentDto.getRewardInfoList().size() > 0) {
                            GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDos.get(i);
                            //插入游戏模板奖励与实际奖品关系
                            GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                            gameRewardRelDo.setTemplateRewardId(gameTemplateRewardDo.getId());
                            gameRewardRelDo.setRewardId(gameRewardDo.getId());
                            gameRewardRelDo.setCreateOper(gameActivityDto.getCreateOper());
                            gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                            gameRewardRelDo.setDeptId(gameActivityDto.getDeptId());
                            rewardRelMapper.insertGameRewardRel(gameRewardRelDo);
                        }
                    }
                }
            }
        }
        return result;
    }

    private void getPools(List<GameRewardPoolDto> pools, GameRewardPoolDto gameRewardPoolDto) {
        if (gameRewardPoolDto.getRewardList() != null && gameRewardPoolDto.getRewardList().size() > 0) {
            GameRewardPoolDto gameRewardPoolDto1 = new GameRewardPoolDto();
            ArrayList<GameRewardDto> gameRewardDtos = new ArrayList<>();
            for (GameRewardDto gameRewardDto : gameRewardPoolDto.getRewardList()) {
                if (!"1".equals(gameRewardDto.getIsDel())) {
                    gameRewardDtos.add(gameRewardDto);
                }
            }
            if (gameRewardDtos.size() > 0) {
                gameRewardPoolDto1.setId(gameRewardPoolDto.getId());
                gameRewardPoolDto1.setIsBackup(gameRewardPoolDto.getIsBackup());
                gameRewardPoolDto1.setBackupPoolType(gameRewardPoolDto.getBackupPoolType());
                gameRewardPoolDto1.setMainPoolId(gameRewardPoolDto.getMainPoolId());
                gameRewardPoolDto1.setMktRightsNum(gameRewardPoolDto.getMktRightsNum());
                gameRewardPoolDto1.setPoolName(gameRewardPoolDto.getPoolName());
                gameRewardPoolDto1.setIsDel(gameRewardPoolDto.getIsDel());
                gameRewardPoolDto1.setRewardList(gameRewardDtos);
                pools.add(gameRewardPoolDto1);
            }
        }
    }

    private void getTurnTablePools(List<TurnTableGameRewardPoolDto> pools, TurnTableGameRewardPoolDto gameRewardPoolDto) {
        TurnTableGameRewardPoolDto gameRewardPoolDto1 = new TurnTableGameRewardPoolDto();
        ArrayList<TurnTableGameRewardDto> gameRewardDtos = new ArrayList<>();
        for (TurnTableGameRewardDto gameRewardDto : gameRewardPoolDto.getRewardList()) {
            if (!"1".equals(gameRewardDto.getIsDel())) {
                gameRewardDtos.add(gameRewardDto);
            }
        }
        if (gameRewardDtos.size() > 0) {
            gameRewardPoolDto1.setId(gameRewardPoolDto.getId());
            gameRewardPoolDto1.setIsBackup(gameRewardPoolDto.getIsBackup());
            gameRewardPoolDto1.setMktRightsNum(gameRewardPoolDto.getMktRightsNum());
            gameRewardPoolDto1.setPoolName(gameRewardPoolDto.getPoolName());
            gameRewardPoolDto1.setIsDel(gameRewardPoolDto.getIsDel());
            gameRewardPoolDto1.setRewardList(gameRewardDtos);

            gameRewardPoolDto1.setRightsWhiteType(gameRewardPoolDto.getRightsWhiteType());
            gameRewardPoolDto1.setIsWhite(gameRewardPoolDto.getIsWhite());
            gameRewardPoolDto1.setMktRightsStatus(gameRewardPoolDto.getMktRightsStatus());
            gameRewardPoolDto1.setMktPlanId(gameRewardPoolDto.getMktPlanId());
            gameRewardPoolDto1.setMktActivityId(gameRewardPoolDto.getMktActivityId());
            gameRewardPoolDto1.setMktRightsMonthType(gameRewardPoolDto.getMktRightsMonthType());
            gameRewardPoolDto1.setMktRightsMonthNum(gameRewardPoolDto.getMktRightsMonthNum());
            gameRewardPoolDto1.setWhiteMktRightsNum(gameRewardPoolDto.getWhiteMktRightsNum());
            gameRewardPoolDto1.setWhiteMktRightsMonthType(gameRewardPoolDto.getWhiteMktRightsMonthType());
            gameRewardPoolDto1.setWhiteMRightsMonthNum(gameRewardPoolDto.getWhiteMRightsMonthNum());
            gameRewardPoolDto1.setJudgeType(gameRewardPoolDto.getJudgeType());
            gameRewardPoolDto1.setQualifyFrom(gameRewardPoolDto.getQualifyFrom());
            gameRewardPoolDto1.setRewardSelect(gameRewardPoolDto.getRewardSelect());

            pools.add(gameRewardPoolDto1);
        }
    }

    private void insertPoolWhiteRihts(TurnTableGameRewardPoolDto poolDto, GameRewardPoolDo gameRewardPoolDo) {
        QualifyJudgeParamDo judgeParamDo = gameRewardPoolisWhite(poolDto);
        qualifyJudgeParamMapper.insertQualifyJudgeParam(judgeParamDo);
        gameRewardPoolDo.setFocusJudgeId(judgeParamDo.getId());
    }

    /**
     * 组装奖池白名单权益参数
     * @param poolDto
     * @return
     */
    private QualifyJudgeParamDo gameRewardPoolisWhite(TurnTableGameRewardPoolDto poolDto) {
        //如果是大转盘活动并且开启白名单，需要记录来源
        QualifyJudgeParamDo judgeParamDo = new QualifyJudgeParamDo();
        judgeParamDo.setJudgeType(poolDto.getJudgeType());
        if (Constants.WHITE_RIGHT_FROM_MKT_NUM.equals(poolDto.getJudgeType())) {
            judgeParamDo.setMktRightsCode(poolDto.getWhiteMktRightsNum());
            judgeParamDo.setMktRightsMonthType(poolDto.getWhiteMktRightsMonthType());
            judgeParamDo.setMktRightsMonthNum(poolDto.getWhiteMRightsMonthNum());
            judgeParamDo.setMktRightsStatus(poolDto.getMktRightsStatus());
        } else if (Constants.WHITE_RIGHT_FROM_MKT_QUA.equals(poolDto.getJudgeType())) {
            judgeParamDo.setMktActivityId(poolDto.getMktActivityId());
            judgeParamDo.setMktPlanId(poolDto.getMktPlanId());
        } else {
            throw new InvalidParameterException("奖池" + poolDto.getPoolName() + "白名单来源异常");
        }
        return judgeParamDo;


    }

    private ResponseData checkRewardPool(List<GameRewardPoolDto> pools,GameActivityDto gameActivityDto) {


        BigDecimal zero = new BigDecimal(0);
        //校验奖池下总概率是否为100%
        for (GameRewardPoolDto gameRewardPoolDto : pools) {
            if(gameRewardPoolDto == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池不能为空");
            }
            if("1".equals(gameRewardPoolDto.getIsBackup())){
                if(StringUtil.isEmpty(gameRewardPoolDto.getBackupPoolType())){
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"备用奖池的奖池类型不能为空");
                }
            }
            List<GameRewardDto> rewardList = gameRewardPoolDto.getRewardList();
            if(rewardList == null || rewardList.size() == 0){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池下的奖励不能为空");
            }
            BigDecimal sumRate = new BigDecimal(0);
            for (GameRewardDto gameRewardDto : rewardList) {
                if(StringUtil.isEmpty(gameRewardDto.getRewardType())){
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型不能为空");
                }
                //校验奖励
                if("1".equals(gameRewardDto.getRewardType())){
                    //刷卡金
                    if(gameRewardDto.getAmount1() == null || gameRewardDto.getAmount1().compareTo(zero) !=1
                            || gameRewardDto.getAmount2() == null || gameRewardDto.getAmount2().compareTo(zero) !=1 ){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为刷卡金,金额区间值必须为大于0的正数");
                    }
                    if(!this.isNumber(gameRewardDto.getAmount1().toString()) || !this.isNumber(gameRewardDto.getAmount2().toString())){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为刷卡金,金额区间值只能是整数或2位小数以内的正数");
                    }
                    if(checkAmount(gameRewardDto.getAmount1()) || checkAmount(gameRewardDto.getAmount2())){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为刷卡金,金额区间值最大值为99999999.99");
                    }
                    if(gameRewardDto.getAmount1().compareTo(gameRewardDto.getAmount2()) == 1){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为刷卡金,左区间必须小于等于右区间");
                    }

                    //  针对天天返现类型活动，刷卡金奖励类型还需要进行 进阶奖励信息的配置校验
                    //  即 game_template_type = hb3-积分转返现 且 gameRewardDto.getRewardType() == 1
                    if(gameActivityDto != null && "hb3".equals(gameActivityDto.getGameTemplateType())){
                        if(StringUtils.isNotEmpty(gameRewardDto.getRedoubleEnable()) && "1".equals(gameRewardDto.getRedoubleEnable())){
                                BigDecimal redoubleRate = new BigDecimal(gameRewardDto.getRedoubleRate());
                            if(gameRewardDto.getRedoubleRate() == null){
                                return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励的概率不能为空");
                            }else if(redoubleRate.compareTo(new BigDecimal(100)) > 0){
                                return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励的概率不能大于100%");
                            }else if(redoubleRate.compareTo(BigDecimal.ZERO) < 0){
                                return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励的概率不能小于0%");
                            }
                            if(StringUtils.isNotEmpty(gameRewardDto.getRedoubleOptions())){
                                if("0".equals(gameRewardDto.getRedoubleOptions())){
                                    if(gameRewardDto.getRedoubleTimes() != null){
                                        if(gameRewardDto.getRedoubleTimes().compareTo(new BigDecimal(1)) <= 0 ){
                                            return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励倍数值不能少于或等于1");
                                        }
                                    }else{
                                        return new ResponseData(ResponseCode.CODE_403.getCode(),"请填入进阶奖励倍数值");
                                    }
                                }else if("1".equals(gameRewardDto.getRedoubleOptions())){
                                    if(gameRewardDto.getRedoubleMinPeriod() != null && gameRewardDto.getRedoubleMaxPeriod() != null){
                                        if(gameRewardDto.getRedoubleMinPeriod().compareTo(BigDecimal.ZERO) < 0 ){
                                            return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励区间值不能小于0");
                                        }else if(gameRewardDto.getRedoubleMaxPeriod().compareTo(BigDecimal.ZERO) < 0){
                                            return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励区间值不能小于0");
                                        }else if(gameRewardDto.getRedoubleMinPeriod().compareTo(gameRewardDto.getRedoubleMaxPeriod()) > 0){
                                            return new ResponseData(ResponseCode.CODE_403.getCode(),"进阶奖励区间左边界值不能大于右边界值");
                                        }
                                    }else{
                                        return new ResponseData(ResponseCode.CODE_403.getCode(),"请填入进阶奖励区间值");
                                    }
                                }else{
                                    return new ResponseData(ResponseCode.CODE_403.getCode(),"请选择进阶奖励倍数或者区间");
                                }
                                if(StringUtils.isNotEmpty(gameRewardDto.getRedoubleLabelText())){
                                    if(gameRewardDto.getRedoubleLabelText().length() > 256){
                                        return new ResponseData(ResponseCode.CODE_403.getCode(),"返现记录-标签文本长度不能超过256");
                                    }
                                }else{
                                    return new ResponseData(ResponseCode.CODE_403.getCode(),"返现记录-标签文本不能为空");
                                }
                                if(StringUtils.isNotEmpty(gameRewardDto.getRedoubleLabelColor())){
                                    if(gameRewardDto.getRedoubleLabelColor().length() > 64){
                                        return new ResponseData(ResponseCode.CODE_403.getCode(),"标签字体颜色长度不能超过64");
                                    }
                                }else{
                                    return new ResponseData(ResponseCode.CODE_403.getCode(),"标签字体颜色不能为空");
                                }
                            }else{
                                return new ResponseData(ResponseCode.CODE_403.getCode(),"请选择进阶奖励倍数或者区间");
                            }

                        }else if(StringUtils.isEmpty(gameRewardDto.getRedoubleEnable())){
                            return new ResponseData(ResponseCode.CODE_403.getCode(),"请选择是否开启进阶奖励");
                        }else if("0".equals(gameRewardDto.getRedoubleEnable())){
                            // 虽然什么都没做，但是不能删。不然为0的时候回被最后一种情况拦截
                        }
                        else{
                            return new ResponseData(ResponseCode.CODE_403.getCode(),"选择开启进阶奖励时，请填入0或者1");
                        }
                    }

                }else if("2".equals(gameRewardDto.getRewardType())){
                    //积分
                    if(gameRewardDto.getIntegral1() == null || gameRewardDto.getIntegral1() < 1
                            || gameRewardDto.getIntegral2() == null || gameRewardDto.getIntegral2() < 1){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为积分,积分区间值必须为大于0的正整数");
                    }
                    if(gameRewardDto.getIntegral1() > gameRewardDto.getIntegral2()){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为积分，左区间必须小于等于右区间");
                    }
                }
                else if("3".equals(gameRewardDto.getRewardType())){
                    //饿了么礼券
                    if(StringUtil.isEmpty(gameRewardDto.getElmeCaseNo())){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型为饿了么礼券,产品编号不能为空");
                    }
                }else{
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖励类型不合法");
                }
                if(!StringUtil.isEmpty(gameRewardDto.getEnterType())){
                    if(!this.isInteger(gameRewardDto.getEnterType())){
                        return new ResponseData(ResponseCode.CODE_403.getCode(),"入账类型必须为正整数");
                    }
                }
                if(StringUtil.isEmpty(gameRewardDto.getRate())){
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"概率不能为空");
                }
                sumRate = sumRate.add(new BigDecimal(gameRewardDto.getRate()));
            }
            if(sumRate.compareTo(new BigDecimal(100)) != 0){
                return new ResponseData(ResponseCode.CODE_403.getCode(), gameRewardPoolDto.getPoolName() + "的奖励概率总合不等100%");
            }

        }
        //校验奖池营管权益编号是否重复
        if(pools.size() > 1){
            TreeSet<GameRewardPoolDto> set = new TreeSet<>((o1, o2) -> o1.getMktRightsNum().compareTo(o2.getMktRightsNum()));
            set.addAll(pools);
            if(set.size() < pools.size()){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池中营管权益编号存在重复");
            }
        }
        return null;
    }

    private ResponseData checkTurnTableRewardPool(List<TurnTableGameRewardPoolDto> pools, List<TurnTableGameRewardInfoDto> rewardInfoList) {


        BigDecimal zero = new BigDecimal(0);
        //校验奖池下总概率是否为100%
        for (TurnTableGameRewardPoolDto gameRewardPoolDto : pools) {

            //奖池基本参数校验
            String poolName = gameRewardPoolDto.getPoolName();
            if (CollectionUtils.isEmpty(gameRewardPoolDto.getRewardList())) {
                //奖池不能为空，奖池内选择的奖品数量不能大于游戏模板绑定的奖品数量
                return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏奖池数量为空");
            }
            if (gameRewardPoolDto.getRewardList().size() > rewardInfoList.size()) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏奖池中存在未在模板配置或模板已删除奖励");
            }
            if(gameRewardPoolDto == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"存在空奖池");
            }
            if (StringUtils.isBlank(gameRewardPoolDto.getPoolName()) || gameRewardPoolDto.getPoolName().length() > 20) {
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】名称或空名称长度超出限制");
            }
            if (StringUtils.isBlank(gameRewardPoolDto.getQualifyFrom())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】抽奖资格来源为空");
            }
            if (StringUtils.isBlank(gameRewardPoolDto.getMktRightsNum()) || gameRewardPoolDto.getMktRightsNum().length() > 30) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】权益编码为空或过长");

            }
            String mktRightsMonthType = gameRewardPoolDto.getMktRightsMonthType();
            if (StringUtils.isBlank(mktRightsMonthType)) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】权益月份类型为空");

            }

            if (("2".equals(mktRightsMonthType) || "3".equals(mktRightsMonthType)) && (gameRewardPoolDto.getMktRightsMonthNum() == null || gameRewardPoolDto.getMktRightsMonthNum() > 99)) {
                //t+n t-n类型，月份数不能为空
                return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】权益月数为空或过大");
            }
            if (StringUtils.isBlank(gameRewardPoolDto.getIsWhite())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】是否开启白名单为空");
            }



            if ( Constants.WHITE_YES.equals(gameRewardPoolDto.getIsWhite())) {
                if (StringUtils.isBlank(gameRewardPoolDto.getRightsWhiteType())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单抽奖方式为空");
                }
                if (StringUtils.isBlank(gameRewardPoolDto.getRewardSelect())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单抽奖奖品选择为空");
                }
                if (StringUtils.isBlank(gameRewardPoolDto.getJudgeType())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单来源为空");
                }
                if (Constants.WHITE_RIGHT_FROM_MKT_NUM.equals(gameRewardPoolDto.getJudgeType())) {
                    if (StringUtils.isBlank(gameRewardPoolDto.getWhiteMktRightsNum()) || gameRewardPoolDto.getWhiteMktRightsNum().length() > 200) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单权益编码为空或过长");
                    }
                    if (StringUtils.isBlank(gameRewardPoolDto.getWhiteMktRightsMonthType())) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单权益月份类型为空");
                    }

                    if (("2".equals(gameRewardPoolDto.getWhiteMktRightsMonthType()) || "3".equals(gameRewardPoolDto.getWhiteMktRightsMonthType())) && (gameRewardPoolDto.getWhiteMRightsMonthNum() == null || gameRewardPoolDto.getWhiteMRightsMonthNum() > 99)) {
                        //t+n t-n类型，月份数不能为空
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单权益月数为空或过大");
                    }

                    if (StringUtils.isBlank(gameRewardPoolDto.getMktRightsStatus())) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单权益状态为空");
                    }
                }

                if (Constants.WHITE_RIGHT_FROM_MKT_QUA.equals(gameRewardPoolDto.getJudgeType())) {
                    String mktActivityId = gameRewardPoolDto.getMktActivityId();
                    if (StringUtils.isBlank(mktActivityId) || mktActivityId.length() > 50) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】活动id为空或过长");
                    }
                    if (!isInteger(mktActivityId)) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单资格达标验证 活动id格式不正确");
                    }
                    String mktPlanId = gameRewardPoolDto.getMktPlanId();
                    if (StringUtils.isBlank(mktPlanId) || !isInteger(mktPlanId) || mktPlanId.length() > 50) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】计划id为空或格式不正确或过长");
                    }
                    if (!isInteger(mktPlanId)) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】白名单资格达标验证 计划id格式不正确");
                    }
                }
            }

            //奖池绑定奖品校验
            List<TurnTableGameRewardDto> rewardList = gameRewardPoolDto.getRewardList();
            if(rewardList == null || rewardList.size() == 0){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】下的奖励不能为空");
            }
            BigDecimal sumRate = new BigDecimal(0);
            for (TurnTableGameRewardDto gameRewardDto : rewardList) {
                String rewardIndex = gameRewardDto.getRewardIndex();
                if (StringUtils.isBlank(rewardIndex)) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池【" + poolName + "】奖品序号不能为空");
                }
                List<TurnTableGameRewardInfoDto> turnTableGameRewardInfoDtos = rewardInfoList.stream().filter(rewardInfo -> rewardIndex.equals(rewardInfo.getRewardIndex()) && !"1".equals(rewardInfo.getIsDel())).collect(Collectors.toList());
                if (CollectionUtils.isEmpty(turnTableGameRewardInfoDtos)) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】下存在活动模板未配置的奖品");
                }
                if (turnTableGameRewardInfoDtos.size() != 1) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】内奖品与模板奖励序号不对应");
                }
                TurnTableGameRewardInfoDto turnTableGameRewardInfoDto = turnTableGameRewardInfoDtos.get(0);
                if (gameRewardDto.getDefaultInventory() == null) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】下的奖励【" + turnTableGameRewardInfoDto.getRewardName() + "】预设库存不能为空");
                }
                if (gameRewardDto.getDefaultInventory().longValue() > 999999) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】下的奖励【" + turnTableGameRewardInfoDto.getRewardName() + "】预设库存超出限制");
                }
                if (gameRewardDto.getRewardUpLimit() == null) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】下的奖励【" + turnTableGameRewardInfoDto.getRewardName() + "】单人中奖上限不能为空");
                }
                if (gameRewardDto.getRewardUpLimit().longValue() > 999999) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】下的奖励【" + turnTableGameRewardInfoDto.getRewardName() + "】单人中奖上限过大");
                }
                String rate = gameRewardDto.getRate();
                if(StringUtil.isEmpty(rate) || (rate.contains(".") && rate.split("\\.")[1].length() > 4)){
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】 奖品【" + turnTableGameRewardInfoDto.getRewardName() + "】概率不能为空或小数位超出限制");
                }
                if (new BigDecimal(rate).compareTo(new BigDecimal(0)) == 0) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池【" + gameRewardPoolDto.getPoolName() + "】 奖品【" + turnTableGameRewardInfoDto.getRewardName() + "】概率不能为0");
                }
                sumRate = sumRate.add(new BigDecimal(rate));
            }
            if(sumRate.compareTo(new BigDecimal(100)) != 0){
                return new ResponseData(ResponseCode.CODE_403.getCode(), gameRewardPoolDto.getPoolName() + "的奖励概率总合不等100%");
            }
        }
        //校验奖池营管权益编号是否重复
        if(pools.size() > 1){
            TreeSet<TurnTableGameRewardPoolDto> set = new TreeSet<>((o1, o2) -> o1.getMktRightsNum().compareTo(o2.getMktRightsNum()));
            set.addAll(pools);
            if(set.size() < pools.size()){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池中营管权益编号存在重复");
            }
        }
        return null;
    }

    /**
     * 正整数
     */
    private boolean isInteger(String str){
        if(StringUtil.isEmpty(str)){
            return false;
        }
        Pattern pattern = Pattern.compile("^[1-9]\\d*$");
        Matcher match = pattern.matcher(str);
        return match.matches();
    }

    /**
     * 校验金额 数据库金额字段定义为10位,所以不允许输入大于99999999.99的值
     * @param amount
     * @return boolean 大于99999999.99返回ture
     */
    private boolean checkAmount(BigDecimal amount){
        if(amount != null){
            BigDecimal limit = new BigDecimal("99999999.99");
            if(amount.compareTo(limit) > 0){
                return true;
            }
        }
        return false;
    }

    /**
     * 正整数或2位小数以内正数(0，0.0，0.00过滤不了)
     */
    private boolean isNumber(String str){
        if(StringUtil.isEmpty(str)){
            return false;
        }
        Pattern pattern = Pattern.compile("^(([1-9]\\d*)|([0]))(\\.(\\d){0,2})?$");
        Matcher match = pattern.matcher(str);
        return match.matches();
    }
   /* private boolean isBlank(Object... args) {
        return Arrays.asList(args).stream().anyMatch(arg -> {
            if (arg instanceof String) {
                return StringUtils.isBlank((String) arg);
            }
            return arg == null;
        });
    }*/

    private String getJudgeType(String mktRightsCode, String mktNameId){
        if(!StringUtil.isEmpty(mktRightsCode) && StringUtil.isEmpty(mktNameId)){
            return QualificationTypeEnum.RIGHTS_AND_INTERESTS_1.getCode();
        }else if(StringUtil.isEmpty(mktRightsCode) && !StringUtil.isEmpty(mktNameId)){
            return QualificationTypeEnum.MARKETING_LIST.getCode();
        }
        return null;
    }

    @Override
    @Transactional(timeout= Constants.TRANSACTIONAL_TIMEOUT)
    public ResponseData insertTurnTableActivityDB(RequestData<TurnTableGameActivityDto> requestData) {
        log.info("新增转盘活动:{}",requestData);
        TurnTableGameActivityDto gameActivityDto = JSON.parseObject(JSON.toJSONString(requestData.getContent()), TurnTableGameActivityDto.class);
        try {
//            gameActivityDto.setGameStatus(Constants.GAME_STATUS_DOWN); //活动状态默认为下架
            //校验数据
            ResponseData responseData = checkTurnTableData(gameActivityDto);
            if (ResponseCode.OK.getCode() != responseData.getCode()) {
                return responseData;
            }
            //插入资格判断记录
            QualifyJudgeParamDo qualifyJudgeParamDo = null;
            if (!StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) || !StringUtil.isEmpty(gameActivityDto.getMktNameId())) {
                qualifyJudgeParamDo = new QualifyJudgeParamDo();
                qualifyJudgeParamDo.setJudgeType(this.getJudgeType(gameActivityDto.getMktRightsCode(), gameActivityDto.getMktNameId()));
                qualifyJudgeParamDo.setMktRightsCode(gameActivityDto.getMktRightsCode());
                qualifyJudgeParamDo.setMktNameId(gameActivityDto.getMktNameId());
                qualifyJudgeParamDo.setCreateOper(gameActivityDto.getCreateOper());
                qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                qualifyJudgeParamDo.setDeptId(gameActivityDto.getDeptId());
                qualifyJudgeParamMapper.insertQualifyJudgeParam(qualifyJudgeParamDo);
            }

            //插入游戏主表数据
            GameActivityDo gameActivityDo = new GameActivityDo();
            gameActivityDo.setGameName(gameActivityDto.getGameName());
            gameActivityDo.setGameTemplateType(gameActivityDto.getGameTemplateType());
            gameActivityDo.setStartTime(gameActivityDto.getStartTime());
            gameActivityDo.setEndTime(gameActivityDto.getEndTime());
            if (qualifyJudgeParamDo != null) {
                gameActivityDo.setFocusJudgeId(qualifyJudgeParamDo.getId());
            }
            gameActivityDo.setIsEnableBkupPool("0");
            gameActivityDo.setCreateOper(gameActivityDto.getCreateOper());
            gameActivityDo.setModifyOper(gameActivityDto.getModifyOper());
            gameActivityDo.setDeptId(gameActivityDto.getDeptId());
            gameActivityDo.setShelfStatus(Constants.GAME_STATUS_DOWN); //默认为下架状态
            gameActivityDo.setEnableWhiteRoster(gameActivityDto.getEnableWhiteRoster());
            gameActivityDo.setRosterRid(gameActivityDto.getRosterRid());
            gameActivityMapper.insertGameActivity(gameActivityDo);

            //插入游戏模板数据
            GameTemplateDo gameTemplateDo = new GameTemplateDo();
            gameTemplateDo.setGameId(gameActivityDo.getId());
            gameTemplateDo.setConfigContent(JSON.toJSONString(gameActivityDto.getConfigContent()));
            gameTemplateDo.setCreateOper(gameActivityDto.getCreateOper());
            gameTemplateDo.setModifyOper(gameActivityDto.getModifyOper());
            gameTemplateDo.setDeptId(gameActivityDto.getDeptId());
            gameTemplateMapper.insertGameTemplate(gameTemplateDo);

            TurnTableConfigContentDto configContentDto = gameActivityDto.getConfigContent();
            List<TurnTableGameRewardInfoDto> rewardInfos = configContentDto.getRewardInfoList();
            //游戏模板奖品列表
            ArrayList<GameTemplateRewardDo> gameTemplateRewardDos = new ArrayList<>();
            if (rewardInfos != null && rewardInfos.size() > 0) {
                for (TurnTableGameRewardInfoDto gameRewardInfoDto : rewardInfos) {
                    //插入游戏模板奖励数据
                    GameTemplateRewardDo gameTemplateRewardDo = new GameTemplateRewardDo();
                    gameTemplateRewardDo.setGameId(gameActivityDo.getId());
                    gameTemplateRewardDo.setGameTemplateId(gameTemplateDo.getId());
                    gameTemplateRewardDo.setRewardName(gameRewardInfoDto.getRewardName());
                    gameTemplateRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                    gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                    gameTemplateRewardDo.setDeptId(gameActivityDto.getDeptId());

                    //转盘添加参数
                    gameTemplateRewardDo.setRewardType(gameRewardInfoDto.getRewardType());
                    gameTemplateRewardDo.setIsRoll(gameRewardInfoDto.getIsRoll());
                    gameTemplateRewardDo.setWinInformWords(gameRewardInfoDto.getWinInformWords());
                    gameTemplateRewardDo.setProLink(gameRewardInfoDto.getProLink());
                    gameTemplateRewardDo.setRightsIde(gameRewardInfoDto.getRightsIde());
                    gameTemplateRewardDo.setMktRightsNum(gameRewardInfoDto.getMktRightsNum());
                    gameTemplateRewardDo.setMktRightsMonthType(gameRewardInfoDto.getMktRightsMonthType());
                    gameTemplateRewardDo.setMktRightsMonthNum(gameRewardInfoDto.getMktRightsMonthNum());
                    gameTemplateRewardDo.setMktRightsTimes(gameRewardInfoDto.getMktRightsTimes());
                    gameTemplateRewardDo.setPropcardType(gameRewardInfoDto.getPropcardType());
                    gameTemplateRewardDo.setAmount(gameRewardInfoDto.getAmount());
                    gameTemplateRewardDo.setEnterType(gameRewardInfoDto.getEnterType());
                    gameTemplateRewardDo.setRightsDesc(gameRewardInfoDto.getRightsDesc());
                    gameTemplateRewardDo.setRightsStatus("0");
                    gameTemplateRewardDo.setRewardIndex(gameRewardInfoDto.getRewardIndex());
                    gameTemplateRewardMapper.insertGameTemplateReward(gameTemplateRewardDo);
                    gameTemplateRewardDos.add(gameTemplateRewardDo);
                }
            }

            List<TurnTableGameRewardPoolDto> poolList = gameActivityDto.getPoolList();
            saveTurnTablePools(gameActivityDto, gameActivityDo, configContentDto, rewardInfos, gameTemplateRewardDos, poolList);
            //t_game_activity_count表创建当天和明天的记录
            String[] dateTimes = {DateUtil.getyyyyMMdd(), DateUtil.getTimeStr("yyyyMMdd", 1)};//当天、明天
            for (String dateTime : dateTimes) {
                GameActivityCountDo activityCountQuery = GameActivityCountDo.builder().gameId(gameActivityDo.getId()).dateTime(dateTime).build();
                List<GameActivityCountDo> activityCountList = gameActivityCountMapper.list(activityCountQuery);
                // 不存在记录
                if (CollectionUtils.isEmpty(activityCountList)) {
                    GameActivityCountDo insertActivityCount = GameActivityCountDo.builder()
                            .gameId(gameActivityDo.getId())
                            .dateTime(dateTime)
                            .receivedAmount(BigDecimal.ZERO)
                            .receivedIntegral(0)
                            .receivedNum(0)
                            .build();
                    gameActivityCountMapper.save(insertActivityCount);
                }
            }

            // 关联活动版本
            activityVersion(gameActivityDo.getId(),gameActivityDto.getId(),gameActivityDto.getVersionId(),gameActivityDto.getNewVersionFlag());

            log.info("新增转盘活动成功");
            return new ResponseData(ResponseCode.OK, gameActivityDo.getId());
        }catch (InvalidParameterException e) {
            log.error(e.getMessage());
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.CODE_403.getCode(), e.getMessage());
        }catch (Exception e){
            log.error("新增转盘活动异常",e);
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.ERROR.getCode(), ResponseCode.ERROR.getMessage());
        }
    }

    /**
     * 补参
     * @param gameActivityDto
     * @return
     */
    private TurnTableGameActivityDto suppParams(TurnTableGameActivityDto gameActivityDto) {
        //将奖池内的奖品参数补全
        //获取模板奖励列表
        List<TurnTableGameRewardInfoDto> rewardTemplates = gameActivityDto.getConfigContent().getRewardInfoList().stream().filter(rewardTempDto -> !"1".equals(rewardTempDto.getIsDel())).collect(Collectors.toList());
        //获取所有奖池的所有奖励
        List<TurnTableGameRewardDto> rewardDtoList = new ArrayList<>();
        for (TurnTableGameRewardPoolDto turnTableGameRewardPoolDto : gameActivityDto.getPoolList()) {
            List<TurnTableGameRewardDto> rewardDtos = turnTableGameRewardPoolDto.getRewardList().stream().filter(rewardTempDto -> !"1".equals(rewardTempDto.getIsDel())).collect(Collectors.toList());
            rewardDtoList.addAll(rewardDtos);
        }

        for (TurnTableGameRewardInfoDto rewardTemplate : rewardTemplates) {
            rewardDtoList.stream().forEach(rewardDto -> {
                if (rewardTemplate.getRewardIndex().equals(rewardDto.getRewardIndex())) {
                    if (Constants.RIGHTS_GAME_CENTER_REWARD_TYPE_CASH.equals(rewardTemplate.getRewardType())) {
                        //如果是刷卡金
                        rewardDto.setAmount1(rewardTemplate.getAmount());
                    }
                    if (Constants.RIGHTS_GAME_CENTER_REWARD_TYPE_PRODUCT.equals(rewardTemplate.getRewardType())
                            && Constants.RIGHTS_FROM_POINTS.equals(rewardTemplate.getRightsIde())) {
                        //如果是实物礼品的特殊积分科目
                        rewardDto.setIntegral1(rewardTemplate.getAmount().intValue());
                    }
                    if (StringUtils.isNotBlank(rewardTemplate.getEnterType())) {
                        rewardDto.setEnterType(rewardTemplate.getEnterType());
                    }
                    if (StringUtils.isNotBlank(rewardTemplate.getRightsDesc())) {
                        rewardDto.setTradeDesc(rewardTemplate.getRightsDesc());
                    }

                    rewardDto.setRewardName(rewardTemplate.getRewardName());
                    rewardDto.setRewardType(rewardTemplate.getRewardType());
                }

            });
        }
        return gameActivityDto;

    }

    private List<GameRewardPoolDo> saveTurnTablePools(TurnTableGameActivityDto gameActivityDto, GameActivityDo gameActivityDo, TurnTableConfigContentDto configContentDto, List<TurnTableGameRewardInfoDto> rewardInfos, ArrayList<GameTemplateRewardDo> gameTemplateRewardDos, List<TurnTableGameRewardPoolDto> poolList) throws InvalidParameterException {
        ArrayList<GameRewardPoolDo> result = new ArrayList<>();
        if(poolList != null && poolList.size() >0){
            for (TurnTableGameRewardPoolDto gameRewardPoolDto :poolList) {
                //插入奖池信息
                GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                gameRewardPoolDo.setGameId(gameActivityDo.getId());
                gameRewardPoolDo.setIsBackup(gameRewardPoolDto.getIsBackup());
                gameRewardPoolDo.setPoolName(gameRewardPoolDto.getPoolName());
                gameRewardPoolDo.setMktRightsNum(gameRewardPoolDto.getMktRightsNum());
                gameRewardPoolDo.setCreateOper(gameActivityDto.getCreateOper());
                gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                gameRewardPoolDo.setDeptId(gameActivityDto.getDeptId());
                //转盘奖池属性
                gameRewardPoolDo.setIsWhite(gameRewardPoolDto.getIsWhite());
                gameRewardPoolDo.setMktRightsMonthType(gameRewardPoolDto.getMktRightsMonthType());
                gameRewardPoolDo.setMktRightsMonthNum(gameRewardPoolDto.getMktRightsMonthNum());

                gameRewardPoolDo.setQualifyFrom(gameRewardPoolDto.getQualifyFrom());


                if (Constants.WHITE_YES.equals(gameRewardPoolDto.getIsWhite())) {
                    //校验选择的rewardIndex是否在模板内存在
                    String[] rewardIndexs = gameRewardPoolDto.getRewardSelect().split(",");
                    for (String rewardIndex : rewardIndexs) {
                        List<TurnTableGameRewardInfoDto> collect = rewardInfos.stream().filter(reward -> rewardIndex.equals(reward.getRewardIndex()) && !"1".equals(reward.getIsDel())).collect(Collectors.toList());
                        if (CollectionUtils.isEmpty(collect)) {
                            //如果当前奖池奖励在模板内不存在
                            throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】白名单选择中存在模板不存在的奖品信息");
                        }
                    }

                    //如果是大转盘活动并且开启白名单，需要记录来源
                    if (Constants.WHITE_RIGHT_SELECT_TYPE_Y.equals(gameRewardPoolDto.getRightsWhiteType()) && gameRewardPoolDto.getRewardSelect().contains(",")) {
                        //如果是百分百抽中，只能单选
                        throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】白名单类型为【百分百抽中】,只能选择一个奖励");
                    }
                    else if (Constants.WHITE_RIGHT_SELECT_TYPE_N.equals(gameRewardPoolDto.getRightsWhiteType()) && rewardIndexs.length >= gameActivityDto.getConfigContent().getRewardInfoList().size()) {
                        throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】白名单类型为【抽不中】,最多只能选择奖励个数为N-1个");
                    }
                    gameRewardPoolDo.setRightsWhiteType(gameRewardPoolDto.getRightsWhiteType());
                    gameRewardPoolDo.setRewardSelect(gameRewardPoolDto.getRewardSelect());
                    insertPoolWhiteRihts(gameRewardPoolDto, gameRewardPoolDo);

                }
                rewardPoolMapper.insertGameRewardPool(gameRewardPoolDo);


                result.add(gameRewardPoolDo);
                List<TurnTableGameRewardDto> rewardList = gameRewardPoolDto.getRewardList();
                if(rewardList != null && rewardList.size() >0){

                    for (TurnTableGameRewardDto rewardInfo : rewardList) {
                        if (StringUtils.isBlank(rewardInfo.getRewardName())) {
                            throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】内奖品名称为空");
                        }
                        if (StringUtils.isBlank(rewardInfo.getRate())) {
                            throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】内奖品[" + rewardInfo.getRewardName() + "]概率为空");

                        }
                        if (rewardInfo.getDefaultInventory() == null) {
                            throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】内奖品[" + rewardInfo.getRewardName() + "]预设库存为空");
                        }
                        if (rewardInfo.getRewardUpLimit() == null) {
                            throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】内奖品[" + rewardInfo.getRewardName() + "]单人中奖上限为空");
                        }

                    }


                    //插入奖励信息
                    for (int i = 0; i< rewardList.size(); i++) {
                        TurnTableGameRewardDto gameRewardDto = rewardList.get(i);
                        GameRewardDo gameRewardDo = new GameRewardDo();
                        gameRewardDo.setGameId(gameActivityDo.getId());
                        gameRewardDo.setPoolId(gameRewardPoolDo.getId());
                        gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                        gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                        BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                        BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                        gameRewardDo.setRate(rate.intValue());
                        gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                        gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                        gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                        gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                        gameRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                        gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                        gameRewardDo.setDeptId(gameActivityDto.getDeptId());

                        //转盘属性
                        gameRewardDo.setRemainderInventory(gameRewardDto.getDefaultInventory()); //初始剩余库存为预设库存
                        gameRewardDo.setDefaultInventory(gameRewardDto.getDefaultInventory());
                        gameRewardDo.setReceivedNum(0); //初始0消耗
                        gameRewardDo.setRewardUpLimit(gameRewardDto.getRewardUpLimit());
                        gameRewardDo.setRewardIndex(gameRewardDto.getRewardIndex());
                        rewardMapper.insertGameReward(gameRewardDo);
                        RedisUtil.set(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameRewardDo.getId(), String.valueOf(gameRewardDto.getDefaultInventory()));
                        //序号转为id补充id
                        gameRewardDto.setId(gameRewardDo.getId());

                        if(configContentDto.getRewardInfoList() != null && configContentDto.getRewardInfoList().size() > 0) {
                            //根据rewardIndex找到gameTemplateRewardDo
                            List<GameTemplateRewardDo> gameTemplateRewardDoList = gameTemplateRewardDos.stream().filter(gameTemplateReward -> gameRewardDo.getRewardIndex().equals(gameTemplateReward.getRewardIndex())).collect(Collectors.toList());
                            if (CollectionUtils.isEmpty(gameTemplateRewardDoList)) {
                                //如果奖池内奖品与当前模板内的奖品不一致（脏数据
                                throw new InvalidParameterException("奖池【" + gameRewardPoolDto.getPoolName() + "】内奖品[" + gameRewardDo.getRewardName() + "]与当前模板内的奖品不一致");
                            }
                            GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList.get(0); //有且只有一个

//                            GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDos.get(i);
                            //插入游戏模板奖励与实际奖品关系
                            GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                            gameRewardRelDo.setTemplateRewardId(gameTemplateRewardDo.getId());
                            gameRewardRelDo.setRewardId(gameRewardDo.getId());
                            gameRewardRelDo.setCreateOper(gameActivityDto.getCreateOper());
                            gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                            gameRewardRelDo.setDeptId(gameActivityDto.getDeptId());
                            rewardRelMapper.insertGameRewardRel(gameRewardRelDo);
                        }

                    }
                }
                if (Constants.WHITE_YES.equals(gameRewardPoolDto.getIsWhite())) {
                    TranslateRewardSelect(gameRewardPoolDo, rewardList);
                }

            }
        }
        return result;
    }

    /**
     * 白名单多选，序号字符串转为id字符串
     * @param gameRewardPoolDo
     * @param rewardList
     * @throws InvalidParameterException
     */
    private void TranslateRewardSelect(GameRewardPoolDo gameRewardPoolDo, List<TurnTableGameRewardDto> rewardList) throws InvalidParameterException {
        //序号转为id
        String rewardSelect = gameRewardPoolDo.getRewardSelect();
        String[] rewardIndex = rewardSelect.split(",");
        StringBuffer strb = new StringBuffer();
        for (TurnTableGameRewardDto rewardDto : rewardList) {
            if (rewardDto.getId() == null) {
                throw new InvalidParameterException("奖池[" + gameRewardPoolDo.getPoolName() + "]白名单权益保存失败！");
            }

            for (String index : rewardIndex) {
                if (rewardDto.getRewardIndex().equals(index) && !"1".equals(rewardDto.getIsDel())) {
                    strb.append(rewardDto.getId()).append(",");
                    continue;
                }
            }
        }
        if (strb.length() == 0) {
            throw new InvalidParameterException("奖池[" + gameRewardPoolDo.getPoolName() + "]白名单奖品选择序号不对应");
        }

        String rewardIds = strb.substring(0, strb.length() - 1);

        if (rewardIndex.length != rewardIds.split(",").length) {
            throw new InvalidParameterException("奖池[" + gameRewardPoolDo.getPoolName() + "]白名单奖品选择序号不对应");
        }

        GameRewardPoolDo poolDo = new GameRewardPoolDo();
        poolDo.setRewardSelect(rewardIds);
        poolDo.setId(gameRewardPoolDo.getId());
        poolDo.setIsDel(gameRewardPoolDo.getIsDel());
        rewardPoolMapper.updateGameRewardPool(poolDo);
    }

    private ResponseData checkTurnTableData(TurnTableGameActivityDto gameActivityDto) {
        if (StringUtil.isEmpty(gameActivityDto.getGameTemplateType())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏模板类型不能为空");
        }

        if (StringUtil.isEmpty(gameActivityDto.getGameName())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏名称不能为空");
        }
        if (gameActivityDto.getGameName().length() > 20) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "游戏名称长度超出限制");

        }

        if (gameActivityDto.getStartTime() == null) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "开始时间不能为空");
        }

        if (gameActivityDto.getEndTime() == null) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "结束时间不能为空");
        }

        if (gameActivityDto.getConfigContent() == null) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "配置内容不能为空");
        }

/*        if (StringUtils.isBlank(gameActivityDto.getGameStatus())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(),"配置内容不能为空");
        }*/

        if (gameActivityDto.getEndTime().before(gameActivityDto.getStartTime())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "开始时间不能大于结束时间");
        }
        if(!StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) && !StringUtil.isEmpty(gameActivityDto.getMktNameId())){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"分众可见的营管权益编码和名单id只能填一个");
        }

        TurnTableConfigContentDto configContent = gameActivityDto.getConfigContent();
        if (StringUtil.isEmpty(configContent.getMainView())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "主觉视图不能为空");
        }

        if (StringUtils.isEmpty(configContent.getPageTitle()) || configContent.getPageTitle().length() > 15)  {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "页面标题为空或长度超限");
        }

        //转盘图片，指针图片不能为空
        if (StringUtils.isBlank(configContent.getTurnTableImg()) || StringUtils.isBlank(configContent.getPointerImg())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "转盘模板转盘图片、指针图片不能为空");
        }
        if (CollectionUtils.isEmpty(configContent.getRewardInfoList())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "转盘模板奖品列表不能为空");
        }
        if (StringUtils.isBlank(configContent.getGameRule())) {
            return new ResponseData(ResponseCode.CODE_403.getCode(), "转盘模板活动规则不能为空");
        }

        ArrayList<TurnTableGameRewardInfoDto> gameRewardInfoDtos = new ArrayList<>();
        for (TurnTableGameRewardInfoDto gameRewardInfoDto :configContent.getRewardInfoList()) {
            if(!"1".equals(gameRewardInfoDto.getIsDel())){
                gameRewardInfoDtos.add(gameRewardInfoDto);
            }
            if ("1".equals(gameRewardInfoDto.getIsDel()) && gameRewardInfoDto.getId() == null) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + gameRewardInfoDto.getRewardName() + "]id为空，无法删除");
            }
        }
        ResponseData responseData = checkGameRewardTemplate(gameRewardInfoDtos);
        if(ResponseCode.OK.getCode() != responseData.getCode()){
            return responseData;
        }
        List<TurnTableGameRewardPoolDto> poolList = gameActivityDto.getPoolList();
        if(poolList == null || poolList.size() == 0){
            return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池列表不能为空");
        }
        List<TurnTableGameRewardPoolDto> mainPools = new ArrayList<>();//主奖池
        for (TurnTableGameRewardPoolDto gameRewardPoolDto : poolList) {
            if("1".equals(gameRewardPoolDto.getIsDel())){
                if (gameRewardPoolDto.getId() == null) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "奖池[" + gameRewardPoolDto.getPoolName() + "]id为空，无法删除");
                }
                continue;
            }
            List<TurnTableGameRewardDto> rewardList = gameRewardPoolDto.getRewardList();
            if (CollectionUtils.isEmpty(rewardList.stream().filter(reward -> !"1".equals(reward.getIsDel())).collect(Collectors.toList()))) {
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池[" + gameRewardPoolDto.getPoolName() + "]内奖励列表不能为空");
            }
            if (rewardList.stream().anyMatch(reward -> "1".equals(reward.getIsDel()) && reward.getId() == null)) {
                return new ResponseData(ResponseCode.CODE_403.getCode(),"奖池[" + gameRewardPoolDto.getPoolName() + "]内待删除奖励id为null");
            }

            //主奖池
            this.getTurnTablePools(mainPools, gameRewardPoolDto);
        }
        //奖池校验
        ResponseData res = this.checkTurnTableRewardPool(mainPools, gameRewardInfoDtos);
        if (res != null){
            return res;
        }
        suppParams(gameActivityDto);
        return new ResponseData(ResponseCode.OK.getCode(), ResponseCode.OK.getMessage());
    }

    private ResponseData checkGameRewardTemplate(List<TurnTableGameRewardInfoDto> rewardInfoList) {

        for (TurnTableGameRewardInfoDto rewardInfo : rewardInfoList) {

            if (StringUtils.isBlank(rewardInfo.getRewardName())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖品名称不能为空");
            }
            if (StringUtils.isBlank(rewardInfo.getRewardIndex())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]序号不能为空");
            }
            if (StringUtils.isBlank(rewardInfo.getRewardType())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]类型不能为空");
            }
            if (!Arrays.asList(Constants.TURN_TABLE_REWARD_TYPE).contains(rewardInfo.getRewardType())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]类型异常");
            }
            if (StringUtils.isBlank(rewardInfo.getIsRoll())) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]是否滚动条展示为空");
            }
            if (rewardInfo.getRewardName().length() > 10) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖品【" + rewardInfo.getRewardName() + "】名称长度超出限制");
            }
            if (StringUtils.isNotBlank(rewardInfo.getWinInformWords()) && rewardInfo.getWinInformWords().length() > 20) {
                return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖品【" + rewardInfo.getRewardName() + "】中奖提示语长度超出限制");
            }

            BigDecimal amount = rewardInfo.getAmount();
            if (Constants.RIGHTS_GAME_CENTER_REWARD_TYPE_CASH.equals(rewardInfo.getRewardType())) {
                if (StringUtils.isBlank(rewardInfo.getRightsIde())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]刷卡金赠送渠道为空");
                }
                
                if (amount == null) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]赠送金额为空");
                }
                if (!isNumber(amount.toString())) {
                    //特殊积分科目整数校验
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]赠送金额格式不正确");
                }
                if (amount.compareTo(new BigDecimal(99999999.99)) > 0) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]赠送金额最大值为99999999.99");
                }
                if (StringUtils.isEmpty(rewardInfo.getEnterType()) || !isInteger(rewardInfo.getEnterType())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]入账类型为空或格式不正确");
                }
                if (StringUtils.isBlank(rewardInfo.getEnterType()) || rewardInfo.getEnterType().length() > 10) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]入账类型为空或长度过长");
                }
                if (StringUtils.isBlank(rewardInfo.getRightsDesc()) || rewardInfo.getRightsDesc().length() > 20) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]交易描述为空或长度超出限制");
                }
            }
            String mktRightsMonthType = rewardInfo.getMktRightsMonthType();
            if (Constants.RIGHTS_GAME_CENTER_REWARD_TYPE_PRODUCT.equals(rewardInfo.getRewardType())) {
                if (StringUtils.isBlank(rewardInfo.getProLink())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]商品链接为空");
                }
                if (rewardInfo.getProLink().length() > 256) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]商品链接长度过长");
                }
                if (StringUtils.isBlank(rewardInfo.getRightsIde())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]购买资格来源为空");
                }

                if (Constants.RIGHTS_FROM_MKT.equals(rewardInfo.getRightsIde())) {
                    if (StringUtils.isBlank(rewardInfo.getMktRightsNum()) || rewardInfo.getMktRightsNum().length() > 30) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益编码为空或长度过长");
                    }
                    if (StringUtils.isBlank(mktRightsMonthType)) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益月份类型为空");
                    }
                    if (("2".equals(mktRightsMonthType) || "3".equals(mktRightsMonthType)) && (rewardInfo.getMktRightsMonthNum() == null || rewardInfo.getMktRightsMonthNum() > 99)) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益月份数为空或过大");
                    }
                    if (rewardInfo.getMktRightsTimes() == null || rewardInfo.getMktRightsTimes() > 999999999) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益可使用次数为空或过大");
                    }
                    if (StringUtils.isBlank(rewardInfo.getRightsStatus())) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益状态为空");
                    }
                }
                else {
                    if (amount == null) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]特殊积分科目积分值为空");
                    }
                    if (!isInteger(amount.toString())) {
                        //特殊积分科目整数校验
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]特殊积分科目积分奖励需为整数");
                    }
                    if (amount.compareTo(new BigDecimal(99999999)) > 0) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]特殊积分科目积分奖励最大值为99999999");
                    }
                    if (StringUtils.isBlank(rewardInfo.getEnterType()) || rewardInfo.getEnterType().length() > 10) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]入账类型为空或长度过长");
                    }
                    if (StringUtils.isBlank(rewardInfo.getRightsDesc()) || rewardInfo.getRightsDesc().length() > 20) {
                        return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]交易描述为空或长度超出限制");
                    }
                }
            }
            if (Constants.RIGHTS_GAME_CENTER_REWARD_TYPE_CONSUME_FUND.equals(rewardInfo.getRewardType())) { //消费金
                if (StringUtils.isBlank(rewardInfo.getRightsIde())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]赠送渠道为空");
                }
                if (StringUtils.isBlank(rewardInfo.getMktRightsNum()) || rewardInfo.getMktRightsNum().length() > 30) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益编码为空或长度过长");
                }
                if (StringUtils.isBlank(mktRightsMonthType)) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益月份类型为空");
                }
                if (("2".equals(mktRightsMonthType) || "3".equals(mktRightsMonthType)) && (rewardInfo.getMktRightsMonthNum() == null || rewardInfo.getMktRightsMonthNum() > 99)) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益月份数为空或过大");
                }
                if (rewardInfo.getMktRightsTimes() == null || rewardInfo.getMktRightsTimes() > 999999999) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益可使用次数为空或过大");
                }
                if (StringUtils.isBlank(rewardInfo.getRightsStatus())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]权益状态为空");
                }
            }
            if (Constants.RIGHTS_GAME_CENTER_REWARD_TYPE_PROP.equals(rewardInfo.getRewardType())) { //道具卡
                if (StringUtils.isBlank(rewardInfo.getPropcardType())) {
                    return new ResponseData(ResponseCode.CODE_403.getCode(), "模板奖励[" + rewardInfo.getRewardName() + "]道具卡触发事件为空");
                }
            }
        }
        return new ResponseData(ResponseCode.OK);
    }

    @Override
    @Transactional(timeout= Constants.TRANSACTIONAL_TIMEOUT)
    public ResponseData updateTurnTableActivityDB(RequestData<TurnTableGameActivityDto> requestData) {
        log.info("修改转盘活动:{}",requestData);
        TurnTableGameActivityDto gameActivityDto = JSON.parseObject(JSON.toJSONString(requestData.getContent()), TurnTableGameActivityDto.class);
        try{
            if(gameActivityDto == null || gameActivityDto.getId() == null){
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动id不能为空");
            }

            GameActivityDo gameActivityDo = gameActivityMapper.queryById(gameActivityDto.getId());
            if (gameActivityDo == null) {
                //获取不到活动信息
                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_ACTIVITY_NOT_EXIST);
            }
            Date now = new Date();
            if (Constants.GAME_STATUS_UP.equals(gameActivityDo.getShelfStatus()) && now.before(gameActivityDo.getEndTime()) && now.after(gameActivityDo.getStartTime())) {
                //活动生效时间内为上架状态，无法修改
                return new ResponseData(ResponseCode.CODE_403.getCode(),"活动生效时间内为上架状态，无法修改");
            }


            //校验数据
            ResponseData responseData = checkTurnTableData(gameActivityDto);
            if(ResponseCode.OK.getCode() != responseData.getCode()){
                return responseData;
            }
            GameActivityDo gameActivityDo1 = new GameActivityDo();
            gameActivityDo1.setId(gameActivityDo.getId());
            gameActivityDo1.setGameName(gameActivityDto.getGameName());
            gameActivityDo1.setStartTime(gameActivityDto.getStartTime());
            gameActivityDo1.setEndTime(gameActivityDto.getEndTime());
            gameActivityDo1.setIsEnableBkupPool("0");
            gameActivityDo1.setModifyOper(gameActivityDto.getModifyOper());
            gameActivityDo1.setShelfStatus(Constants.GAME_STATUS_DOWN); //修改后需要手动上线，默认为下架状态
            gameActivityDo1.setRosterRid(gameActivityDto.getRosterRid());
            gameActivityDo1.setEnableWhiteRoster(gameActivityDto.getEnableWhiteRoster());
            //更新资格判断记录
            QualifyJudgeParamDo qualifyJudgeParamDo = null;
            if(gameActivityDo.getFocusJudgeId() != null){
                qualifyJudgeParamDo = qualifyJudgeParamMapper.queryById(gameActivityDo.getFocusJudgeId());
                if(qualifyJudgeParamDo == null){
                    //资格判断信息不存在
                    return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_QUALIFY_JUDGE_PARAM_NOT_EXIST);
                }
                if(StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) && StringUtil.isEmpty(gameActivityDto.getMktNameId())){
                    //删除资格判断记录
                    qualifyJudgeParamDo.setIsDel("1");
                    qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                    qualifyJudgeParamMapper.delQualifyJudgeParam(qualifyJudgeParamDo);
                    gameActivityDo1.setFocusJudgeId(null);
                }else{
                    //更新资格判断记录
                    QualifyJudgeParamDo updateQualifyJudgeParam = new QualifyJudgeParamDo();
                    updateQualifyJudgeParam.setId(qualifyJudgeParamDo.getId());
                    updateQualifyJudgeParam.setJudgeType(this.getJudgeType(gameActivityDto.getMktRightsCode(),gameActivityDto.getMktNameId()));
                    updateQualifyJudgeParam.setMktRightsCode(gameActivityDto.getMktRightsCode()==null?"":gameActivityDto.getMktRightsCode());
                    updateQualifyJudgeParam.setMktNameId(gameActivityDto.getMktNameId()==null?"":gameActivityDto.getMktNameId());
                    updateQualifyJudgeParam.setModifyOper(gameActivityDto.getModifyOper());
                    qualifyJudgeParamMapper.updateQualifyJudgeParam(updateQualifyJudgeParam);
                    gameActivityDo1.setFocusJudgeId(updateQualifyJudgeParam.getId());
                }
            }else{
                if(!StringUtil.isEmpty(gameActivityDto.getMktRightsCode()) || !StringUtil.isEmpty(gameActivityDto.getMktNameId())){
                    //新增资格判断记录
                    qualifyJudgeParamDo = new QualifyJudgeParamDo();
                    qualifyJudgeParamDo.setJudgeType(this.getJudgeType(gameActivityDto.getMktRightsCode(),gameActivityDto.getMktNameId()));
                    qualifyJudgeParamDo.setMktRightsCode(gameActivityDto.getMktRightsCode());
                    qualifyJudgeParamDo.setMktNameId(gameActivityDto.getMktNameId());
                    qualifyJudgeParamDo.setCreateOper(gameActivityDto.getCreateOper());
                    qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                    qualifyJudgeParamDo.setDeptId(gameActivityDto.getDeptId());
                    qualifyJudgeParamMapper.insertQualifyJudgeParam(qualifyJudgeParamDo);
                    gameActivityDo1.setFocusJudgeId(qualifyJudgeParamDo.getId());
                }
            }
            //更新活动
            gameActivityMapper.updateGameActivity(gameActivityDo1);

            GameTemplateDo gameTemplateDo = gameTemplateMapper.queryByGameId(gameActivityDto.getId());
            if(gameTemplateDo == null){
                //获取不到活动模板信息
                throw new IllegalArgumentException("活动模板信息不存在");
//                return new ResponseData(ResponseCode.RIGHTS_GAME_CENTER_GAME_TEMPLATE_NOT_EXIST);
            }

            List<GameTemplateRewardDo> gameTemplateRewardDos = gameTemplateRewardMapper.queryByGameIdAndGameTemplateId(gameActivityDo.getId(), gameTemplateDo.getId());

            TurnTableConfigContentDto configContentDto = gameActivityDto.getConfigContent();
            List<TurnTableGameRewardInfoDto> rewardInfos = configContentDto.getRewardInfoList();
            List<TurnTableGameRewardInfoDto> rewardInfoList = new ArrayList<>();
            //游戏模板奖品列表
            ArrayList<GameTemplateRewardDo> gameTemplateRewardDoList = new ArrayList<>();
            ArrayList<GameTemplateRewardDo> gameTemplateRewardDoUpdateList = new ArrayList<>();
            if(rewardInfos != null && rewardInfos.size() > 0){
                for (TurnTableGameRewardInfoDto gameRewardInfoDto :rewardInfos) {
                    GameTemplateRewardDo gameTemplateRewardDo = new GameTemplateRewardDo();
                    if(gameRewardInfoDto.getId() == null ){
                        //插入游戏模板奖励数据
                        gameTemplateRewardDo.setGameId(gameActivityDo.getId());
                        gameTemplateRewardDo.setGameTemplateId(gameTemplateDo.getId());
                        gameTemplateRewardDo.setRewardName(gameRewardInfoDto.getRewardName());
                        gameTemplateRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                        gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                        gameTemplateRewardDo.setDeptId(gameActivityDto.getDeptId());

                        //转盘添加参数
                        gameTemplateRewardDo.setRewardType(gameRewardInfoDto.getRewardType());
                        gameTemplateRewardDo.setIsRoll(gameRewardInfoDto.getIsRoll());
                        gameTemplateRewardDo.setWinInformWords(gameRewardInfoDto.getWinInformWords());
                        gameTemplateRewardDo.setProLink(gameRewardInfoDto.getProLink());
                        gameTemplateRewardDo.setRightsIde(gameRewardInfoDto.getRightsIde());
                        gameTemplateRewardDo.setMktRightsNum(gameRewardInfoDto.getMktRightsNum());
                        gameTemplateRewardDo.setMktRightsMonthType(gameRewardInfoDto.getMktRightsMonthType());
                        gameTemplateRewardDo.setMktRightsMonthNum(gameRewardInfoDto.getMktRightsMonthNum());
                        gameTemplateRewardDo.setMktRightsTimes(gameRewardInfoDto.getMktRightsTimes());
                        gameTemplateRewardDo.setPropcardType(gameRewardInfoDto.getPropcardType());
                        gameTemplateRewardDo.setAmount(gameRewardInfoDto.getAmount());
                        gameTemplateRewardDo.setEnterType(gameRewardInfoDto.getEnterType());
                        gameTemplateRewardDo.setRewardIndex(gameRewardInfoDto.getRewardIndex());
                        gameTemplateRewardDo.setRightsStatus(gameRewardInfoDto.getRightsStatus());
                        gameTemplateRewardDo.setRightsDesc(gameRewardInfoDto.getRightsDesc());

                        gameTemplateRewardMapper.insertGameTemplateReward(gameTemplateRewardDo);
                        gameTemplateRewardDoList.add(gameTemplateRewardDo);
                        TurnTableGameRewardInfoDto rewardInfoTmp = new TurnTableGameRewardInfoDto();
                        rewardInfoTmp.setId(gameTemplateRewardDo.getId());
                        rewardInfoTmp.setRewardName(gameTemplateRewardDo.getRewardName());
                        rewardInfoTmp.setIsDel("0");
                        rewardInfoList.add(rewardInfoTmp);
                    }else{
                        if("1".equals(gameRewardInfoDto.getIsDel())){
                            gameTemplateRewardDo.setId(gameRewardInfoDto.getId());
                            gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            gameTemplateRewardDo.setIsDel("1");
                            gameTemplateRewardMapper.delGameTemplateReward(gameTemplateRewardDo);
                        }else{
                            //修改
                            gameTemplateRewardDo.setRewardName(gameRewardInfoDto.getRewardName());
                            gameTemplateRewardDo.setId(gameRewardInfoDto.getId());
                            gameTemplateRewardDo.setModifyOper(gameActivityDto.getModifyOper());

                            //转盘添加参数
                            gameTemplateRewardDo.setRewardType(gameRewardInfoDto.getRewardType());
                            gameTemplateRewardDo.setIsRoll(gameRewardInfoDto.getIsRoll());
                            gameTemplateRewardDo.setWinInformWords(gameRewardInfoDto.getWinInformWords());
                            gameTemplateRewardDo.setProLink(gameRewardInfoDto.getProLink());
                            gameTemplateRewardDo.setRightsIde(gameRewardInfoDto.getRightsIde());
                            gameTemplateRewardDo.setMktRightsNum(gameRewardInfoDto.getMktRightsNum());
                            gameTemplateRewardDo.setMktRightsMonthType(gameRewardInfoDto.getMktRightsMonthType());
                            gameTemplateRewardDo.setMktRightsMonthNum(gameRewardInfoDto.getMktRightsMonthNum());
                            gameTemplateRewardDo.setMktRightsTimes(gameRewardInfoDto.getMktRightsTimes());
                            gameTemplateRewardDo.setPropcardType(gameRewardInfoDto.getPropcardType());
                            gameTemplateRewardDo.setAmount(gameRewardInfoDto.getAmount());
                            gameTemplateRewardDo.setEnterType(gameRewardInfoDto.getEnterType());
                            gameTemplateRewardDo.setRewardIndex(gameRewardInfoDto.getRewardIndex());
                            gameTemplateRewardDo.setRightsStatus(gameRewardInfoDto.getRightsStatus());
                            gameTemplateRewardDo.setRightsDesc(gameRewardInfoDto.getRightsDesc());

                            gameTemplateRewardMapper.updateGameTemplateReward(gameTemplateRewardDo);
                            gameTemplateRewardDoList.add(gameTemplateRewardDo);
                            rewardInfoList.add(gameRewardInfoDto);
                        }
                    }
                    gameTemplateRewardDoUpdateList.add(gameTemplateRewardDo);
                }
            }
            //更新游戏模板
            TurnTableConfigContentDto configContentTmp = new TurnTableConfigContentDto();
            configContentTmp.setPageTitle(configContentDto.getPageTitle());
            configContentTmp.setGameRule(configContentDto.getGameRule());
            configContentTmp.setMainView(configContentDto.getMainView());
            configContentTmp.setPointerImg(configContentDto.getPointerImg());
            configContentTmp.setTurnTableImg(configContentDto.getTurnTableImg());
            configContentTmp.setRewardInfoList(rewardInfoList);
            GameTemplateDo gameTemplateDo1 = new GameTemplateDo();
            BeanUtils.copyProperties(gameActivityDto,gameTemplateDo1);
            gameTemplateDo1.setId(gameTemplateDo.getId());
            gameTemplateDo1.setConfigContent(JSON.toJSONString(configContentTmp));
            gameTemplateMapper.updateGameTemplate(gameTemplateDo1);

            //主奖池
            List<TurnTableGameRewardPoolDto> poolList = gameActivityDto.getPoolList();
            turnTablePoolOperate(gameActivityDto, gameActivityDo, gameTemplateRewardDos, gameTemplateRewardDoList, gameTemplateRewardDoUpdateList, poolList, null);
            return new ResponseData(ResponseCode.OK,gameActivityDo.getId());
        } catch (IllegalArgumentException e) {
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.CODE_403.getCode(), e.getMessage());
        }
        catch (Exception e){
            log.error("修改转盘活动异常",e);
            //事务回滚
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ResponseData(ResponseCode.ERROR.getCode(), ResponseCode.ERROR.getMessage());
        }

    }

    public List<GameRewardPoolDo> turnTablePoolOperate(TurnTableGameActivityDto gameActivityDto, GameActivityDo gameActivityDo, List<GameTemplateRewardDo> gameTemplateRewardDos, ArrayList<GameTemplateRewardDo> gameTemplateRewardDoList, ArrayList<GameTemplateRewardDo> gameTemplateRewardDoUpdateList, List<TurnTableGameRewardPoolDto> poolList, List<GameRewardPoolDo> mainPool) throws Exception {
        List<TurnTableGameRewardPoolDto> insertPool = new ArrayList<>();
        List<TurnTableGameRewardPoolDto> deletePool = new ArrayList<>();
        List<TurnTableGameRewardPoolDto> updatePool = new ArrayList<>();
        List<GameRewardPoolDo> pools = new ArrayList<>();//新增或修改奖池，存至pools
        if(poolList != null && poolList.size() > 0){
            for (TurnTableGameRewardPoolDto pool :poolList) {
                if(pool.getId() == null){
                    insertPool.add(pool);
                }else{
                    if("1".equals(pool.getIsDel())){
                        deletePool.add(pool);
                    }else{
                        updatePool.add(pool);
                    }
                }
            }
        }


        try {
            //删除奖池
            if(deletePool.size() > 0){
                for (TurnTableGameRewardPoolDto poolDto : deletePool) {
                    List<TurnTableGameRewardDto> rewardList = poolDto.getRewardList();
                    if(rewardList != null && rewardList.size() > 0){
                        for(int i = 0; i< rewardList.size(); i++){
                            TurnTableGameRewardDto gameRewardDto = rewardList.get(i);
                            if(gameTemplateRewardDos != null && gameTemplateRewardDos.size() > 0) {
                                //根据rewardIndex找到gameTemplateRewardDo
                                List<GameTemplateRewardDo> gameTemplateRewardDoList1 = gameTemplateRewardDoUpdateList.stream().filter(gameTemplateReward -> gameRewardDto.getRewardIndex().equals(gameTemplateReward.getRewardIndex())).collect(Collectors.toList());
                                if (CollectionUtils.isEmpty(gameTemplateRewardDoList)) {
                                    //如果奖池内奖品与当前模板内的奖品不一致（脏数据
                                    throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】内奖品[" + gameRewardDto.getRewardName() + "]与当前模板内的奖品不一致");
                                }
                                GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList1.get(0); //有且只有一个
//                                GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDos.get(i);
                                List<GameRewardRelDo> gameRewardRelDos = rewardRelMapper.queryByTemplateRewardIdAndRewardId(gameTemplateRewardDo.getId(), gameRewardDto.getId());
                                if (gameRewardRelDos != null && gameRewardRelDos.size() > 0) {
                                    for (GameRewardRelDo gameRewardRelDo : gameRewardRelDos) {
                                        //删除游戏模板奖励与实际奖品关系
                                        gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                        gameRewardRelDo.setIsDel("1");
                                        rewardRelMapper.delGameRewardRel(gameRewardRelDo);
                                    }
                                }
                            }

                            //删除奖池下的奖励信息
                            GameRewardDo gameRewardDo = new GameRewardDo();
                            gameRewardDo.setId(gameRewardDto.getId());
                            gameRewardDo.setIsDel("1");
                            gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            rewardMapper.delGameReward(gameRewardDo);
                            //删除redis库存
                            RedisUtil.del(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameRewardDto.getId());

                        }
                    }

                    if (Constants.WHITE_YES.equals(poolDto.getIsWhite())) {
                        //删除白名单权益
                        Long focusJudgeId = poolDto.getFocusJudgeId();
                        QualifyJudgeParamDo qualifyJudgeParamDo = new QualifyJudgeParamDo();
                        qualifyJudgeParamDo.setId(focusJudgeId);
                        qualifyJudgeParamDo.setIsDel("1");
                        qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                        qualifyJudgeParamMapper.delQualifyJudgeParam(qualifyJudgeParamDo);
                    }

                    //删除奖池
                    GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                    gameRewardPoolDo.setId(poolDto.getId());
                    gameRewardPoolDo.setIsDel("1");
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                    rewardPoolMapper.delGameRewardPool(gameRewardPoolDo);
                }
            }




            //新增奖池
            if(insertPool.size() > 0){
                for (TurnTableGameRewardPoolDto poolDto : insertPool) {
                    //插入奖池信息
                    GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                    gameRewardPoolDo.setGameId(gameActivityDto.getId());
                    gameRewardPoolDo.setIsBackup(poolDto.getIsBackup());
                    if("1".equals(poolDto.getIsBackup())){
                        for (GameRewardPoolDo tmp : mainPool) {
                            if(poolDto.getMktRightsNum().equals(tmp.getMktRightsNum())){
                                gameRewardPoolDo.setMainPoolId(tmp.getId());
                            }
                        }
                    }
                    gameRewardPoolDo.setPoolName(poolDto.getPoolName());
                    gameRewardPoolDo.setMktRightsNum(poolDto.getMktRightsNum());
                    gameRewardPoolDo.setCreateOper(gameActivityDto.getCreateOper());
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                    gameRewardPoolDo.setDeptId(gameActivityDto.getDeptId());
                    //转盘奖池属性
                    gameRewardPoolDo.setIsWhite(poolDto.getIsWhite());
                    gameRewardPoolDo.setMktRightsMonthType(poolDto.getMktRightsMonthType());
                    gameRewardPoolDo.setMktRightsMonthNum(poolDto.getMktRightsMonthNum());
                    gameRewardPoolDo.setRightsWhiteType(poolDto.getRightsWhiteType());
                    gameRewardPoolDo.setRewardSelect(poolDto.getRewardSelect());
                    gameRewardPoolDo.setQualifyFrom(poolDto.getQualifyFrom());

                    if (Constants.WHITE_YES.equals(poolDto.getIsWhite())) {
                        //校验选择的rewardIndex是否在模板内存在
                        String[] rewardIndexs = poolDto.getRewardSelect().split(",");
                        for (String rewardIndex : rewardIndexs) {
                            List<GameTemplateRewardDo> collect = gameTemplateRewardDoList.stream().filter(reward -> rewardIndex.equals(reward.getRewardIndex()) && !"1".equals(reward.getIsDel())).collect(Collectors.toList());
                            if (CollectionUtils.isEmpty(collect)) {
                                //如果当前奖池奖励在模板内不存在
                                throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】白名单选择中存在模板不存在的奖品信息");
                            }
                        }

                        //如果是大转盘活动并且开启白名单，需要记录来源
                        if (Constants.WHITE_RIGHT_SELECT_TYPE_Y.equals(poolDto.getRightsWhiteType()) && poolDto.getRewardSelect().contains(",")) {
                            //如果是百分百抽中，只能单选
                            throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】白名单类型为【百分百抽中】,只能选择一个奖励");
                        }
                        else if (Constants.WHITE_RIGHT_SELECT_TYPE_N.equals(poolDto.getRightsWhiteType()) && rewardIndexs.length >= gameActivityDto.getConfigContent().getRewardInfoList().size()) {
                            throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】白名单类型为【抽不中】,最多只能选择奖励个数为N-1个");
                        }

                        insertPoolWhiteRihts(poolDto, gameRewardPoolDo);
                    }
                    rewardPoolMapper.insertGameRewardPool(gameRewardPoolDo);
                    pools.add(gameRewardPoolDo);
                    List<TurnTableGameRewardDto> rewardList = poolDto.getRewardList();
                    if(rewardList != null && rewardList.size() > 0){
                        //插入奖励信息
                        for (int i = 0; i< rewardList.size(); i++) {
                            TurnTableGameRewardDto gameRewardDto = rewardList.get(i);
                            GameRewardDo gameRewardDo = new GameRewardDo();
                            gameRewardDo.setGameId(gameActivityDo.getId());
                            gameRewardDo.setPoolId(gameRewardPoolDo.getId());
                            gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                            gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                            BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                            BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                            gameRewardDo.setRate(rate.intValue());
                            gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                            gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                            gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                            gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                            gameRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                            gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                            gameRewardDo.setDeptId(gameActivityDto.getDeptId());
                            //转盘属性
                            gameRewardDo.setRemainderInventory(gameRewardDto.getDefaultInventory());
                            gameRewardDo.setDefaultInventory(gameRewardDto.getDefaultInventory());
                            gameRewardDo.setReceivedNum(0);
                            gameRewardDo.setRewardUpLimit(gameRewardDto.getRewardUpLimit());
                            gameRewardDo.setRewardIndex(gameRewardDto.getRewardIndex());
                            rewardMapper.insertGameReward(gameRewardDo);
                            RedisUtil.set(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() +  gameRewardDo.getId(), String.valueOf(gameRewardDto.getDefaultInventory()));

                            gameRewardDto.setId(gameRewardDo.getId());

                            if(gameTemplateRewardDoList.size() > 0) {
//                                GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList.get(i);
                                //根据rewardIndex找到gameTemplateRewardDo
                                List<GameTemplateRewardDo> gameTemplateRewardDoList1 = gameTemplateRewardDoList.stream().filter(gameTemplateReward -> gameRewardDo.getRewardIndex().equals(gameTemplateReward.getRewardIndex())).collect(Collectors.toList());
                                if (CollectionUtils.isEmpty(gameTemplateRewardDoList)) {
                                    //如果奖池内奖品与当前模板内的奖品不一致（脏数据
                                    throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】内奖品[" + gameRewardDo.getRewardName() + "]与当前模板内的奖品不一致");

                                }
                                GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList1.get(0); //有且只有一个

                                //插入游戏模板奖励与实际奖品关系
                                GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                                gameRewardRelDo.setTemplateRewardId(gameTemplateRewardDo.getId());
                                gameRewardRelDo.setRewardId(gameRewardDo.getId());
                                gameRewardRelDo.setCreateOper(gameActivityDto.getCreateOper());
                                gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                gameRewardRelDo.setDeptId(gameActivityDto.getDeptId());
                                rewardRelMapper.insertGameRewardRel(gameRewardRelDo);
                            }
                        }
                    }

                    //todo:多选奖品id转换
                    if (Constants.WHITE_YES.equals(gameRewardPoolDo.getIsWhite())) {
                        TranslateRewardSelect(gameRewardPoolDo, rewardList);
                    }
                }
            }

            //修改奖池
            if(updatePool.size() > 0){
                for (TurnTableGameRewardPoolDto poolDto : updatePool) {
                    GameRewardPoolDo gameRewardPoolDo = new GameRewardPoolDo();
                    gameRewardPoolDo.setId(poolDto.getId());
                    gameRewardPoolDo.setIsBackup(poolDto.getIsBackup());
                    gameRewardPoolDo.setPoolName(poolDto.getPoolName());
                    gameRewardPoolDo.setMktRightsNum(poolDto.getMktRightsNum());
                    gameRewardPoolDo.setModifyOper(gameActivityDto.getModifyOper());
                    //转盘奖池属性
                    gameRewardPoolDo.setIsWhite(poolDto.getIsWhite());
                    gameRewardPoolDo.setMktRightsMonthType(poolDto.getMktRightsMonthType());
                    gameRewardPoolDo.setMktRightsMonthNum(poolDto.getMktRightsMonthNum());
                    gameRewardPoolDo.setRightsWhiteType(poolDto.getRightsWhiteType());
                    gameRewardPoolDo.setRewardSelect(poolDto.getRewardSelect());
                    gameRewardPoolDo.setQualifyFrom(poolDto.getQualifyFrom());

                    if (Constants.WHITE_YES.equals(poolDto.getIsWhite())) {
                        //校验选择的rewardIndex是否在模板内存在
                        String[] rewardIndexs = poolDto.getRewardSelect().split(",");
                        for (String rewardIndex : rewardIndexs) {
                            List<GameTemplateRewardDo> collect = gameTemplateRewardDoList.stream().filter(reward -> rewardIndex.equals(reward.getRewardIndex()) && !"1".equals(reward.getIsDel())).collect(Collectors.toList());
                            if (CollectionUtils.isEmpty(collect)) {
                                //如果当前奖池奖励在模板内不存在
                                throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】白名单选择中存在模板不存在的奖品信息");
                            }
                        }

                        //如果是大转盘活动并且开启白名单，需要记录来源
                        if (Constants.WHITE_RIGHT_SELECT_TYPE_Y.equals(poolDto.getRightsWhiteType()) && poolDto.getRewardSelect().contains(",")) {
                            //如果是百分百抽中，只能单选
                            throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】白名单类型为【百分百抽中】,只能选择一个奖励");
                        }
                        else if (Constants.WHITE_RIGHT_SELECT_TYPE_N.equals(poolDto.getRightsWhiteType()) && rewardIndexs.length >= gameActivityDto.getConfigContent().getRewardInfoList().size()) {
                            throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】白名单类型为【抽不中】,最多只能选择奖励个数为N-1个");
                        }
                        QualifyJudgeParamDo judgeParamDo = gameRewardPoolisWhite(poolDto);
                        if (poolDto.getFocusJudgeId() == null) { //从未启用白名单到启用白名单
                            insertPoolWhiteRihts(poolDto, gameRewardPoolDo);
                        }
                        else { //白名单设置修改
                            judgeParamDo.setId(poolDto.getFocusJudgeId());
                            judgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                            qualifyJudgeParamMapper.updateQualifyJudgeParam(judgeParamDo);
                        }
                    }
                    else if (poolDto.getFocusJudgeId() != null) { //表示从启用白名单到关闭白名单
                        //删除白名单权益
                        Long focusJudgeId = poolDto.getFocusJudgeId();
                        QualifyJudgeParamDo qualifyJudgeParamDo = new QualifyJudgeParamDo();
                        qualifyJudgeParamDo.setId(focusJudgeId);
                        qualifyJudgeParamDo.setIsDel("1");
                        qualifyJudgeParamDo.setModifyOper(gameActivityDto.getModifyOper());
                        qualifyJudgeParamMapper.delQualifyJudgeParam(qualifyJudgeParamDo);
                        gameRewardPoolDo.setFocusJudgeId(null);
                    }


                    rewardPoolMapper.updateGameRewardPool(gameRewardPoolDo);
                    pools.add(gameRewardPoolDo);
                    List<TurnTableGameRewardDto> rewardList = poolDto.getRewardList();

                    if(rewardList != null && rewardList.size() >0){
                        List<GameRewardDo> gameRewardDos = rewardMapper.queryByGameIdAndPoolId(gameActivityDo.getId(), gameRewardPoolDo.getId());
                        for (int i = 0; i< rewardList.size(); i++) {
                            TurnTableGameRewardDto gameRewardDto = rewardList.get(i);

                            if(gameRewardDto.getId() == null){
                                //新增奖池奖励
                                //todo 由于前端传参可能异常，需增加能否新增奖池校验
                                if (gameRewardDos.stream().anyMatch(gameReward -> gameRewardDto.getRewardIndex().equals(gameReward.getRewardIndex()) && !"1".equals(gameRewardDto.getIsDel()))) {
                                    //当前数据库已经存在了正常的该奖品序号的奖品。（奖池内的奖品只能从模板绑定的奖励里面选择，并且无法重复选择同一个模板奖励）
                                    List<TurnTableGameRewardDto> collect = rewardList.stream().filter(rewardDto -> gameRewardDto.getRewardIndex().equals(rewardDto.getRewardIndex()) && "1".equals(rewardDto.getIsDel())).collect(Collectors.toList());
                                    //报文中已删除的该奖品序号奖池奖品，正常有且只有一个
                                    if (CollectionUtils.isEmpty(collect)) { //数据库已存在该rewardIndex数据且请求报文内没有删除申请
                                        throw new IllegalArgumentException("奖池【" + gameRewardPoolDo.getPoolName() + "】已存在奖品【" + gameRewardDto.getRewardName() + "】,无法新增");
                                    }
                                }


                                GameRewardDo gameRewardDo = new GameRewardDo();
                                gameRewardDo.setGameId(gameActivityDo.getId());
                                gameRewardDo.setPoolId(gameRewardPoolDo.getId());
                                gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                                gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                                BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                                BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                                gameRewardDo.setRate(rate.intValue());
                                gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                                gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                                gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                                gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                                gameRewardDo.setCreateOper(gameActivityDto.getCreateOper());
                                gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                                gameRewardDo.setDeptId(gameActivityDto.getDeptId());
                                //转盘属性
                                gameRewardDo.setRemainderInventory(gameRewardDto.getDefaultInventory());
                                gameRewardDo.setDefaultInventory(gameRewardDto.getDefaultInventory());
                                gameRewardDo.setReceivedNum(0);
                                gameRewardDo.setRewardUpLimit(gameRewardDto.getRewardUpLimit());
                                gameRewardDo.setRewardIndex(gameRewardDto.getRewardIndex());
                                rewardMapper.insertGameReward(gameRewardDo);

                                gameRewardDto.setId(gameRewardDo.getId());

                                RedisUtil.set(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() +  gameRewardDo.getId(), String.valueOf(gameRewardDto.getDefaultInventory()));
                                if(gameTemplateRewardDoUpdateList.size() > 0) {
//                                    GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoUpdateList.get(i);

                                    //根据rewardIndex找到gameTemplateRewardDo
                                    List<GameTemplateRewardDo> gameTemplateRewardDoList1 = gameTemplateRewardDoUpdateList.stream().filter(gameTemplateReward -> gameRewardDo.getRewardIndex().equals(gameTemplateReward.getRewardIndex())).collect(Collectors.toList());
                                    if (CollectionUtils.isEmpty(gameTemplateRewardDoList)) {
                                        //如果奖池内奖品与当前模板内的奖品不一致（脏数据
                                        throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】内奖品[" + gameRewardDo.getRewardName() + "]与当前模板内的奖品不一致");

                                    }
                                    GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList1.get(0); //有且只有一个
                                    //插入游戏模板奖励与实际奖品关系
                                    GameRewardRelDo gameRewardRelDo = new GameRewardRelDo();
                                    gameRewardRelDo.setTemplateRewardId(gameTemplateRewardDo.getId());
                                    gameRewardRelDo.setRewardId(gameRewardDo.getId());
                                    gameRewardRelDo.setCreateOper(gameActivityDto.getCreateOper());
                                    gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                    gameRewardRelDo.setDeptId(gameActivityDto.getDeptId());
                                    rewardRelMapper.insertGameRewardRel(gameRewardRelDo);
                                }
                            }else{
                                if("1".equals(gameRewardDto.getIsDel())){
                                    //删除
                                    if(gameTemplateRewardDoUpdateList.size() > 0) {
                                        List<GameRewardRelDo> gameRewardRelDos = null;
                                        //根据rewardIndex找到gameTemplateRewardDo
                                        List<GameTemplateRewardDo> gameTemplateRewardDoList1 = gameTemplateRewardDoUpdateList.stream().filter(gameTemplateReward -> gameRewardDto.getRewardIndex().equals(gameTemplateReward.getRewardIndex())).collect(Collectors.toList());
                                        if (CollectionUtils.isEmpty(gameTemplateRewardDoList1)) {
                                            //如果奖池内奖品与当前模板内的奖品不一致（脏数据
//                                            throw new InvalidParameterException("奖池【" + poolDto.getPoolName() + "】内奖品[" + gameRewardDto.getRewardName() + "]与当前模板内的奖品不一致");
                                            gameRewardRelDos = rewardRelMapper.queryByRewardId(gameRewardDto.getId());
                                        }
                                        else {
                                            GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoList1.get(0); //有且只有一个
                                            gameRewardRelDos = rewardRelMapper.queryByTemplateRewardIdAndRewardId(gameTemplateRewardDo.getId(), gameRewardDto.getId());
                                        }

//                                        GameTemplateRewardDo gameTemplateRewardDo = gameTemplateRewardDoUpdateList.get(i);
                                        if (gameRewardRelDos != null && gameRewardRelDos.size() > 0) {
                                            for (GameRewardRelDo gameRewardRelDo : gameRewardRelDos) {
                                                //删除游戏模板奖励与实际奖品关系
                                                gameRewardRelDo.setModifyOper(gameActivityDto.getModifyOper());
                                                gameRewardRelDo.setIsDel("1");
                                                rewardRelMapper.delGameRewardRel(gameRewardRelDo);
                                            }
                                        }
                                    }
                                    //删除奖池下的奖励信息
                                    GameRewardDo gameRewardDo = new GameRewardDo();
                                    gameRewardDo.setId(gameRewardDto.getId());
                                    gameRewardDo.setIsDel("1");
                                    gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                                    rewardMapper.delGameReward(gameRewardDo);
                                }else{
                                    //修改

                                    GameRewardDo gameRewardDo = new GameRewardDo();
                                    gameRewardDo.setId(gameRewardDto.getId());
                                    gameRewardDo.setRewardName(gameRewardDto.getRewardName());
                                    gameRewardDo.setRewardType(gameRewardDto.getRewardType());
                                    BigDecimal bigDecimal = new BigDecimal(gameRewardDto.getRate());
                                    BigDecimal rate = bigDecimal.multiply(new BigDecimal(10000));
                                    gameRewardDo.setRate(rate.intValue());
                                    gameRewardDo.setAmount1(gameRewardDto.getAmount1());
                                    gameRewardDo.setIntegral1(gameRewardDto.getIntegral1());
                                    gameRewardDo.setEnterType(gameRewardDto.getEnterType());
                                    gameRewardDo.setTradeDesc(gameRewardDto.getTradeDesc());
                                    gameRewardDo.setModifyOper(gameActivityDto.getModifyOper());
                                    //转盘属性
                                    Long newDefaultInventory = gameRewardDto.getDefaultInventory();
                                    GameRewardDo gameReward = rewardMapper.queryById(gameRewardDto.getId());
                                    if (gameReward == null) {
                                        throw new IllegalArgumentException("奖池【" + gameRewardPoolDo.getPoolName() + "】奖品【" + gameRewardDto.getRewardName() + "】获取异常");
                                    }
                                    Long oldDefaultInventory = gameReward.getDefaultInventory();
                                    String redisNum = RedisUtil.get(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameReward.getId()); //缓存剩余库存数量
                                    long offset = newDefaultInventory - oldDefaultInventory;
                                    long newRemainderInventory = gameReward.getRemainderInventory() + offset;

                                    if (newDefaultInventory.compareTo(oldDefaultInventory) != 0) {
                                        //预设库存变动，需要重新设置已消耗库存和剩余库存
                                        if ((new BigDecimal(newRemainderInventory).compareTo(new BigDecimal(0)) < 0)) { //新的剩余库存不能为负
                                            throw new IllegalArgumentException("库存设置不能小于已消耗库存");
                                        }
                                        gameRewardDo.setRemainderInventory(gameReward.getRemainderInventory() + offset);
                                        gameRewardDo.setDefaultInventory(newDefaultInventory);

                                        if (StringUtils.isBlank(redisNum)) { //redis 的key不存在，重新设置为剩余库存
                                            RedisUtil.set(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameReward.getId(), String.valueOf(newRemainderInventory));
                                        }
                                        else {
                                            RedisUtil.incr(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameReward.getId(), offset); //调整redis库存
                                        }
                                    }
                                    else if (StringUtils.isBlank(redisNum)) { //redis 的key不存在，重新设置为剩余库存
                                        RedisUtil.set(RedisKeyPrefix.RIGHTS_TURNTABLE_INVENTORY.getPrefixName() + gameReward.getId(), String.valueOf(newRemainderInventory));
                                    }

                                    gameRewardDo.setRewardUpLimit(gameRewardDto.getRewardUpLimit());
                                    rewardMapper.updateGameReward(gameRewardDo);
                                }
                            }
                        }

                        //修改完校验再次校验奖池总概率
                        //统计数据库当前奖池的概率，如果为100，则不能新增
                        List<GameRewardDo> gameRewardDos1 = rewardMapper.queryByGameIdAndPoolId(gameActivityDo.getId(), gameRewardPoolDo.getId());
                        int rateSum = gameRewardDos1.stream().mapToInt(GameRewardDo::getRate).sum();
                        if (rateSum != 1000000) { //数据库内已经100%概率
                            throw new IllegalArgumentException("奖池【" + gameRewardPoolDo.getPoolName() + "】概率总和不等于100%");
                        }
                        // todo :修改奖池时，无论是增加奖品，删除奖品，都需要调整rewardSelect。如果没有新增或删除，则无需调整rewardSelect
                        //此时rewardSelect 为逗号间隔的序号
                        if (Constants.WHITE_YES.equals(gameRewardPoolDo.getIsWhite())) {
                            TranslateRewardSelect(gameRewardPoolDo, rewardList);
                        }

                    }
                }
            }
        }
        catch (IllegalArgumentException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(e.getMessage());
        }
        catch (Exception e) {
            log.error("转盘奖池修改异常", e.getMessage(), e);
            throw new Exception(e);
        }

        return pools;
    }

}
