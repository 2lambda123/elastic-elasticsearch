/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.xcore {
    requires org.elasticsearch.cli;
    requires org.elasticsearch.base;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.server;
    requires org.elasticsearch.sslconfig;
    requires org.elasticsearch.xcontent;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.apache.lucene.join;
    requires unboundid.ldapsdk;

    exports org.elasticsearch.index.engine.frozen;
    exports org.elasticsearch.license;
    exports org.elasticsearch.license.internal;// to org.elasticsearch.stateless;
    exports org.elasticsearch.protocol.xpack.common;
    exports org.elasticsearch.protocol.xpack.frozen;
    exports org.elasticsearch.protocol.xpack.graph;
    exports org.elasticsearch.protocol.xpack.license;
    exports org.elasticsearch.protocol.xpack.watcher;
    exports org.elasticsearch.protocol.xpack;
    exports org.elasticsearch.snapshots.sourceonly;
    exports org.elasticsearch.xpack.cluster.action;
    exports org.elasticsearch.xpack.cluster.routing.allocation.mapper;
    exports org.elasticsearch.xpack.cluster.routing.allocation;
    exports org.elasticsearch.xpack.core;
    exports org.elasticsearch.xpack.core.action.util;
    exports org.elasticsearch.xpack.core.action;
    exports org.elasticsearch.xpack.core.aggregatemetric;
    exports org.elasticsearch.xpack.core.analytics.action;
    exports org.elasticsearch.xpack.core.analytics;
    exports org.elasticsearch.xpack.core.archive;
    exports org.elasticsearch.xpack.core.async;
    exports org.elasticsearch.xpack.core.ccr.action;
    exports org.elasticsearch.xpack.core.ccr;
    exports org.elasticsearch.xpack.core.common.notifications;
    exports org.elasticsearch.xpack.core.common.search.aggregations;
    exports org.elasticsearch.xpack.core.common.socket;
    exports org.elasticsearch.xpack.core.common.stats;
    exports org.elasticsearch.xpack.core.common.table;
    exports org.elasticsearch.xpack.core.common.time;
    exports org.elasticsearch.xpack.core.common.validation;
    exports org.elasticsearch.xpack.core.common;
    exports org.elasticsearch.xpack.core.datastreams;
    exports org.elasticsearch.xpack.core.deprecation;
    exports org.elasticsearch.xpack.core.downsample;
    exports org.elasticsearch.xpack.core.enrich.action;
    exports org.elasticsearch.xpack.core.enrich;
    exports org.elasticsearch.xpack.core.eql;
    exports org.elasticsearch.xpack.core.frozen.action;
    exports org.elasticsearch.xpack.core.frozen;
    exports org.elasticsearch.xpack.core.graph.action;
    exports org.elasticsearch.xpack.core.graph;
    exports org.elasticsearch.xpack.core.ilm.action;
    exports org.elasticsearch.xpack.core.ilm.step.info;
    exports org.elasticsearch.xpack.core.ilm;
    exports org.elasticsearch.xpack.core.indexing;
    exports org.elasticsearch.xpack.core.logstash;
    exports org.elasticsearch.xpack.core.ml.action;
    exports org.elasticsearch.xpack.core.ml.annotations;
    exports org.elasticsearch.xpack.core.ml.calendars;
    exports org.elasticsearch.xpack.core.ml.datafeed.extractor;
    exports org.elasticsearch.xpack.core.ml.datafeed;
    exports org.elasticsearch.xpack.core.ml.dataframe.analyses;
    exports org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;
    exports org.elasticsearch.xpack.core.ml.dataframe.evaluation.common;
    exports org.elasticsearch.xpack.core.ml.dataframe.evaluation.outlierdetection;
    exports org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;
    exports org.elasticsearch.xpack.core.ml.dataframe.evaluation;
    exports org.elasticsearch.xpack.core.ml.dataframe.explain;
    exports org.elasticsearch.xpack.core.ml.dataframe.stats.classification;
    exports org.elasticsearch.xpack.core.ml.dataframe.stats.common;
    exports org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;
    exports org.elasticsearch.xpack.core.ml.dataframe.stats.regression;
    exports org.elasticsearch.xpack.core.ml.dataframe.stats;
    exports org.elasticsearch.xpack.core.ml.dataframe;
    exports org.elasticsearch.xpack.core.ml.inference.assignment;
    exports org.elasticsearch.xpack.core.ml.inference.persistence;
    exports org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;
    exports org.elasticsearch.xpack.core.ml.inference.preprocessing;
    exports org.elasticsearch.xpack.core.ml.inference.results;
    exports org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;
    exports org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;
    exports org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;
    exports org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;
    exports org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;
    exports org.elasticsearch.xpack.core.ml.inference.trainedmodel;
    exports org.elasticsearch.xpack.core.ml.inference.utils;
    exports org.elasticsearch.xpack.core.ml.inference;
    exports org.elasticsearch.xpack.core.ml.job.config;
    exports org.elasticsearch.xpack.core.ml.job.groups;
    exports org.elasticsearch.xpack.core.ml.job.messages;
    exports org.elasticsearch.xpack.core.ml.job.persistence;
    exports org.elasticsearch.xpack.core.ml.job.process.autodetect.output;
    exports org.elasticsearch.xpack.core.ml.job.process.autodetect.state;
    exports org.elasticsearch.xpack.core.ml.job.results;
    exports org.elasticsearch.xpack.core.ml.job.snapshot.upgrade;
    exports org.elasticsearch.xpack.core.ml.notifications;
    exports org.elasticsearch.xpack.core.ml.process.writer;
    exports org.elasticsearch.xpack.core.ml.stats;
    exports org.elasticsearch.xpack.core.ml.utils.time;
    exports org.elasticsearch.xpack.core.ml.utils;
    exports org.elasticsearch.xpack.core.ml;
    exports org.elasticsearch.xpack.core.monitoring.action;
    exports org.elasticsearch.xpack.core.monitoring.exporter;
    exports org.elasticsearch.xpack.core.monitoring;
    exports org.elasticsearch.xpack.core.rest.action;
    exports org.elasticsearch.xpack.core.rollup.action;
    exports org.elasticsearch.xpack.core.rollup.job;
    exports org.elasticsearch.xpack.core.rollup;
    exports org.elasticsearch.xpack.core.scheduler;
    exports org.elasticsearch.xpack.core.search.action;
    exports org.elasticsearch.xpack.core.searchablesnapshots;
    exports org.elasticsearch.xpack.core.security.action.apikey;
    exports org.elasticsearch.xpack.core.security.action.enrollment;
    exports org.elasticsearch.xpack.core.security.action.oidc;
    exports org.elasticsearch.xpack.core.security.action.privilege;
    exports org.elasticsearch.xpack.core.security.action.profile;
    exports org.elasticsearch.xpack.core.security.action.realm;
    exports org.elasticsearch.xpack.core.security.action.role;
    exports org.elasticsearch.xpack.core.security.action.rolemapping;
    exports org.elasticsearch.xpack.core.security.action.saml;
    exports org.elasticsearch.xpack.core.security.action.service;
    exports org.elasticsearch.xpack.core.security.action.token;
    exports org.elasticsearch.xpack.core.security.action.user;
    exports org.elasticsearch.xpack.core.security.action;
    exports org.elasticsearch.xpack.core.security.authc.esnative;
    exports org.elasticsearch.xpack.core.security.authc.file;
    exports org.elasticsearch.xpack.core.security.authc.jwt;
    exports org.elasticsearch.xpack.core.security.authc.kerberos;
    exports org.elasticsearch.xpack.core.security.authc.ldap.support;
    exports org.elasticsearch.xpack.core.security.authc.ldap;
    exports org.elasticsearch.xpack.core.security.authc.oidc;
    exports org.elasticsearch.xpack.core.security.authc.pki;
    exports org.elasticsearch.xpack.core.security.authc.saml;
    exports org.elasticsearch.xpack.core.security.authc.service;
    exports org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;
    exports org.elasticsearch.xpack.core.security.authc.support.mapper;
    exports org.elasticsearch.xpack.core.security.authc.support;
    exports org.elasticsearch.xpack.core.security.authc;
    exports org.elasticsearch.xpack.core.security.authz.accesscontrol;
    exports org.elasticsearch.xpack.core.security.authz.permission;
    exports org.elasticsearch.xpack.core.security.authz.privilege;
    exports org.elasticsearch.xpack.core.security.authz.store;
    exports org.elasticsearch.xpack.core.security.authz.support;
    exports org.elasticsearch.xpack.core.security.authz;
    exports org.elasticsearch.xpack.core.security.support;
    exports org.elasticsearch.xpack.core.security.user;
    exports org.elasticsearch.xpack.core.security.xcontent;
    exports org.elasticsearch.xpack.core.security;
    exports org.elasticsearch.xpack.core.slm.action;
    exports org.elasticsearch.xpack.core.slm;
    exports org.elasticsearch.xpack.core.spatial.action;
    exports org.elasticsearch.xpack.core.spatial.search.aggregations;
    exports org.elasticsearch.xpack.core.spatial;
    exports org.elasticsearch.xpack.core.sql;
    exports org.elasticsearch.xpack.core.ssl.action;
    exports org.elasticsearch.xpack.core.ssl.cert;
    exports org.elasticsearch.xpack.core.ssl.rest;
    exports org.elasticsearch.xpack.core.ssl;
    exports org.elasticsearch.xpack.core.template;
    exports org.elasticsearch.xpack.core.termsenum.action;
    exports org.elasticsearch.xpack.core.termsenum.rest;
    exports org.elasticsearch.xpack.core.textstructure.action;
    exports org.elasticsearch.xpack.core.textstructure.structurefinder;
    exports org.elasticsearch.xpack.core.transform.action;
    exports org.elasticsearch.xpack.core.transform.notifications;
    exports org.elasticsearch.xpack.core.transform.transforms.latest;
    exports org.elasticsearch.xpack.core.transform.transforms.persistence;
    exports org.elasticsearch.xpack.core.transform.transforms.pivot;
    exports org.elasticsearch.xpack.core.transform.transforms;
    exports org.elasticsearch.xpack.core.transform.utils;
    exports org.elasticsearch.xpack.core.transform;
    exports org.elasticsearch.xpack.core.upgrade;
    exports org.elasticsearch.xpack.core.votingonly;
    exports org.elasticsearch.xpack.core.watcher.actions.throttler;
    exports org.elasticsearch.xpack.core.watcher.actions;
    exports org.elasticsearch.xpack.core.watcher.client;
    exports org.elasticsearch.xpack.core.watcher.common.secret;
    exports org.elasticsearch.xpack.core.watcher.common.stats;
    exports org.elasticsearch.xpack.core.watcher.condition;
    exports org.elasticsearch.xpack.core.watcher.crypto;
    exports org.elasticsearch.xpack.core.watcher.execution;
    exports org.elasticsearch.xpack.core.watcher.history;
    exports org.elasticsearch.xpack.core.watcher.input.none;
    exports org.elasticsearch.xpack.core.watcher.input;
    exports org.elasticsearch.xpack.core.watcher.support.xcontent;
    exports org.elasticsearch.xpack.core.watcher.support;
    exports org.elasticsearch.xpack.core.watcher.transform.chain;
    exports org.elasticsearch.xpack.core.watcher.transform;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.ack;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.activate;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.delete;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.execute;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.get;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.put;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.service;
    exports org.elasticsearch.xpack.core.watcher.transport.actions.stats;
    exports org.elasticsearch.xpack.core.watcher.transport.actions;
    exports org.elasticsearch.xpack.core.watcher.trigger;
    exports org.elasticsearch.xpack.core.watcher.watch;
    exports org.elasticsearch.xpack.core.watcher;
}
