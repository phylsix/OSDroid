#!/usr/bin/env python
"""Given a document, extract features and make predictions with ML techniques.
    This document will not have `metadata` embeded by stompAMQ
"""

import os
import json
import time
import statistics
from collections import OrderedDict
from os.path import join, dirname, abspath

import numpy as np
import pandas as pd
import xgboost as xgb
from monitutils import get_yamlconfig, fmttime, update_prediction_history_db

CONFIG_FILE_PATH = join(dirname(abspath(__file__)), 'config/config.yml')


# ------------------------------------------------------------------------------


def get_time_sinceOpenInHour(doc):
    transitions = doc["transitions"]
    runningOpen = [cell for cell in transitions if cell.get("Status", None) == "running-open"]
    runningOpenTime = None
    if runningOpen:
        runningOpenTime = runningOpen[0].get("UpdateTime", None)

    if runningOpenTime:
        return (int(time.time()) - runningOpenTime) / 60.0 / 60.0
    else:
        return np.nan


# ------------------------------------------------------------------------------


def get_failureRate(doc):
    return doc["failureRate"]


# ------------------------------------------------------------------------------


def get_totalError(doc):
    return doc["totalError"]


# ------------------------------------------------------------------------------


def get_sites_siteCounts(doc):
    tasks = doc["tasks"]
    siteErrors = [tsk.get("siteErrors", []) for tsk in tasks]
    siteErrors = [sitecount for t in siteErrors for sitecount in t]

    siteErrorsUnique = dict()
    for se in siteErrors:
        siteErrorsUnique[se["site"]] = siteErrorsUnique.get(se["site"], 0) + se["counts"]
    return len(siteErrorsUnique) if siteErrorsUnique else 0


# ------------------------------------------------------------------------------


def get_type_encoded(doc):
    wtype = doc["type"]
    if wtype == "TaskChain":
        return 0
    elif wtype == "ReReco":
        return 1
    elif wtype == "StepChain":
        return 2
    else:
        return -1


# ------------------------------------------------------------------------------


def get_sites_errorPerSite_max(doc):
    tasks = doc["tasks"]
    siteErrors = [tsk.get("siteErrors", []) for tsk in tasks]
    siteErrors = [sitecount for t in siteErrors for sitecount in t]

    siteErrorsUnique = dict()
    for se in siteErrors:
        siteErrorsUnique[se["site"]] = siteErrorsUnique.get(se["site"], 0) + se["counts"]
    errorPerSite = list(siteErrorsUnique.values())
    return max(errorPerSite) if siteErrorsUnique else -1


# ------------------------------------------------------------------------------


def get_sites_errorPerSite_min(doc):
    tasks = doc["tasks"]
    siteErrors = [tsk.get("siteErrors", []) for tsk in tasks]
    siteErrors = [sitecount for t in siteErrors for sitecount in t]

    siteErrorsUnique = dict()
    for se in siteErrors:
        siteErrorsUnique[se["site"]] = siteErrorsUnique.get(se["site"], 0) + se["counts"]
    errorPerSite = list(siteErrorsUnique.values())
    return min(errorPerSite) if siteErrorsUnique else -1


# ------------------------------------------------------------------------------


def get_sites_errorPerSite_median(doc):
    tasks = doc["tasks"]
    siteErrors = [tsk.get("siteErrors", []) for tsk in tasks]
    siteErrors = [sitecount for t in siteErrors for sitecount in t]

    siteErrorsUnique = dict()
    for se in siteErrors:
        siteErrorsUnique[se["site"]] = siteErrorsUnique.get(se["site"], 0) + se["counts"]
    errorPerSite = list(siteErrorsUnique.values())
    return statistics.median(errorPerSite) if siteErrorsUnique else -1.0


# ------------------------------------------------------------------------------


def get_sites_errorPerSite_mean(doc):
    tasks = doc["tasks"]
    siteErrors = [tsk.get("siteErrors", []) for tsk in tasks]
    siteErrors = [sitecount for t in siteErrors for sitecount in t]

    siteErrorsUnique = dict()
    for se in siteErrors:
        siteErrorsUnique[se["site"]] = siteErrorsUnique.get(se["site"], 0) + se["counts"]
    errorPerSite = list(siteErrorsUnique.values())
    return statistics.mean(errorPerSite) if siteErrorsUnique else -1.0


# ------------------------------------------------------------------------------


def get_sites_errorPerSite_stdDev(doc):
    tasks = doc["tasks"]
    siteErrors = [tsk.get("siteErrors", []) for tsk in tasks]
    siteErrors = [sitecount for t in siteErrors for sitecount in t]

    siteErrorsUnique = dict()
    for se in siteErrors:
        siteErrorsUnique[se["site"]] = siteErrorsUnique.get(se["site"], 0) + se["counts"]
    errorPerSite = list(siteErrorsUnique.values())
    return statistics.stdev(errorPerSite) if len(siteErrorsUnique) > 1 else -1.0


# ------------------------------------------------------------------------------


def get_errorCode_primary_multiplicity(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    primaryCodesUnique = dict()
    for err in errors:
        primaryCodesUnique[err["errorCode"]] = (
            primaryCodesUnique.get(err["errorCode"], 0) + err["counts"]
        )
    return len(primaryCodesUnique)


# ------------------------------------------------------------------------------


def get_errorCode_primary_leadingCode(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    primaryCodesUnique = dict()
    for err in errors:
        primaryCodesUnique[err["errorCode"]] = (
            primaryCodesUnique.get(err["errorCode"], 0) + err["counts"]
        )
    prim_leadingCode = -1
    if primaryCodesUnique:
        primaryCodeSorted = sorted(primaryCodesUnique.items(), key=lambda kv: kv[1], reverse=True)
        prim_leadingCode = int(primaryCodeSorted[0][0])
    return prim_leadingCode


# ------------------------------------------------------------------------------


def get_errorCode_primary_leadingRatio(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    primaryCodesUnique = dict()
    for err in errors:
        primaryCodesUnique[err["errorCode"]] = (
            primaryCodesUnique.get(err["errorCode"], 0) + err["counts"]
        )
    prim_leadingRatio = -1.0
    if primaryCodesUnique:
        primaryCodeSorted = sorted(primaryCodesUnique.items(), key=lambda kv: kv[1], reverse=True)
        prim_leadingRatio = primaryCodeSorted[0][1] / sum(list(primaryCodesUnique.values()))
    return prim_leadingRatio


# ------------------------------------------------------------------------------


def get_errorCode_secondary_multiplicity(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    secondaryCodeUnique = dict()
    for err in errors:
        if err["secondaryErrorCodes"]:
            for serr in err["secondaryErrorCodes"]:
                secondaryCodeUnique[serr] = secondaryCodeUnique.get(serr, 0) + err["counts"]
    return len(secondaryCodeUnique)


# ------------------------------------------------------------------------------


def get_errorCode_secondary_leadingCode(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    secondaryCodeUnique = dict()
    for err in errors:
        if err["secondaryErrorCodes"]:
            for serr in err["secondaryErrorCodes"]:
                secondaryCodeUnique[serr] = secondaryCodeUnique.get(serr, 0) + err["counts"]
    secd_leadingCode = -1
    if secondaryCodeUnique:
        secondaryCodeSorted = sorted(
            secondaryCodeUnique.items(), key=lambda kv: kv[1], reverse=True
        )
        secd_leadingCode = int(secondaryCodeSorted[0][0])
    return secd_leadingCode


# ------------------------------------------------------------------------------


def get_errorCode_secondary_leadingRatio(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    secondaryCodeUnique = dict()
    for err in errors:
        if err["secondaryErrorCodes"]:
            for serr in err["secondaryErrorCodes"]:
                secondaryCodeUnique[serr] = secondaryCodeUnique.get(serr, 0) + err["counts"]
    secd_leadingRatio = -1.0
    if secondaryCodeUnique:
        secondaryCodeSorted = sorted(secondaryCodeUnique.items(),
                                     key=lambda kv: kv[1], reverse=True)
        secd_leadingRatio = secondaryCodeSorted[0][1] / sum(list(secondaryCodeUnique.values()))
    return secd_leadingRatio


# ------------------------------------------------------------------------------


def get_errorKeywords_multiplicity(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    errorKeywordsUnique = dict()
    for err in errors:
        if err["errorKeywords"]:
            for kw in err["errorKeywords"]:
                errorKeywordsUnique[kw] = errorKeywordsUnique.get(kw, 0) + err["counts"]
    return len(errorKeywordsUnique)


# ------------------------------------------------------------------------------


def get_errorKeywords_leading_encoded(doc):
    """
    error keywords are strings, to pipe into ML training and inference,
    they need to be transformed as numbers.
    LabelEncoder works for training, but not able to group similar
    errorkeywords together. And does not work during inference. For now
    a rule-based approach is taken. Describled below:
    ---
    These errorkeywords are extracted from logs if they contain some
    *buzzword*, like error(s), failure(s), failed. So we first get rid
    of those, and also sep char like -_, and group by other keywords:

        step, submit, report, job, log, rss, assert, performance,
        fileopen, hlt, reco, script, event

    , each stratified by 1000, added by the length of the rest chars.
    If an errorkeyword does not have any above keyword, then it's just
     encoded as the length of the rest chars, should fall into range
    [0, 1000). Here we assume the length of an errorkeyword should be
    no more than 1000.
    """

    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    errorKeywordsUnique = dict()
    for err in errors:
        if err["errorKeywords"]:
            for kw in err["errorKeywords"]:
                errorKeywordsUnique[kw] = errorKeywordsUnique.get(kw, 0) + err["counts"]
    kwrd_leading = ""
    if errorKeywordsUnique:
        errorKeywordsSorted = sorted(
            errorKeywordsUnique.items(), key=lambda kv: kv[1], reverse=True
        )
        kwrd_leading = errorKeywordsSorted[0][0]

    # encoding
    ekwrd = (
        kwrd_leading.lower()
        .replace("-", "")
        .replace("_", "")
        .replace("errors", "")
        .replace("error", "")
        .replace("failures", "")
        .replace("failure", "")
        .replace("failed", "")
        .replace("fail", "")
    )
    signaturekwrd = [
        "step", "submit", "report", "job",
        "log", "rss", "assert", "performance",
        "fileopen", "hlt", "reco", "script", "event",
    ]

    totalweight = 0
    for w, sk in enumerate(signaturekwrd, 1):
        if sk in ekwrd:
            totalweight += 1000 * w
            ekwrd = ekwrd.replace(sk, "")
    if ekwrd:
        totalweight += len(ekwrd)

    return totalweight


# ------------------------------------------------------------------------------


def get_errorKeywords_leadingRatio(doc):
    tasks = doc["tasks"]
    errors = [tsk.get("errors", []) for tsk in tasks]
    errors = [e for t in errors for e in t]

    errorKeywordsUnique = dict()
    for err in errors:
        if err["errorKeywords"]:
            for kw in err["errorKeywords"]:
                errorKeywordsUnique[kw] = errorKeywordsUnique.get(kw, 0) + err["counts"]
    kwrd_leadingRatio = -1.0
    if errorKeywordsUnique:
        errorKeywordsSorted = sorted(errorKeywordsUnique.items(),
                                     key=lambda kv: kv[1], reverse=True)
        kwrd_leadingRatio = errorKeywordsSorted[0][1] / sum(list(errorKeywordsUnique.values()))
    return kwrd_leadingRatio


# ------------------------------------------------------------------------------


def extract_doc(sdoc):
    wname = sdoc["name"]
    featureExtracted = OrderedDict()

    featureExtracted["failureRate"] = get_failureRate(sdoc)
    featureExtracted["totalError"] = get_totalError(sdoc)
    featureExtracted["sites_siteCounts"] = get_sites_siteCounts(sdoc)
    featureExtracted["type"] = get_type_encoded(sdoc)
    featureExtracted["sites_errorPerSite_max"] = get_sites_errorPerSite_max(sdoc)
    featureExtracted["sites_errorPerSite_min"] = get_sites_errorPerSite_min(sdoc)
    featureExtracted["sites_errorPerSite_median"] = get_sites_errorPerSite_median(sdoc)
    featureExtracted["sites_errorPerSite_mean"] = get_sites_errorPerSite_mean(sdoc)
    featureExtracted["sites_errorPerSite_stdDev"] = get_sites_errorPerSite_stdDev(sdoc)
    featureExtracted["errorCode_primary_multiplicity"] = get_errorCode_primary_multiplicity(sdoc)
    featureExtracted["errorCode_primary_leadingCode"] = get_errorCode_primary_leadingCode(sdoc)
    featureExtracted["errorCode_primary_leadingRatio"] = get_errorCode_primary_leadingRatio(sdoc)
    featureExtracted["errorCode_secondary_multiplicity"] = get_errorCode_secondary_multiplicity(sdoc)
    featureExtracted["errorCode_secondary_leadingCode"] = get_errorCode_secondary_leadingCode(sdoc)
    featureExtracted["errorCode_secondary_leadingRatio"] = get_errorCode_secondary_leadingRatio(sdoc)
    featureExtracted["errorKeywords_multiplicity"] = get_errorKeywords_multiplicity(sdoc)
    featureExtracted["errorKeywords_leading"] = get_errorKeywords_leading_encoded(sdoc)
    featureExtracted["errorKeywords_leadingRatio"] = get_errorKeywords_leadingRatio(sdoc)
    featureExtracted["time_sinceOpenInHour"] = get_time_sinceOpenInHour(sdoc)

    return wname, featureExtracted


# ------------------------------------------------------------------------------


def predict_docs(docs, model):

    if not docs:
        return {}
    features = []
    for doc in docs:
        wname, featureExtracted = extract_doc(doc)
        featureExtracted["name"] = wname
        features.append(featureExtracted)
    df = pd.DataFrame(features)
    feature_cols = [c for c in df.columns if c != "name"]
    X = df[feature_cols]
    Xxg = xgb.DMatrix(X)

    bst = xgb.Booster({"nthread": 4})
    bst.load_model(model)
    predprob = bst.predict(Xxg).reshape(X.shape[0], 3)

    res = dict(zip(df["name"].values, predprob.tolist()))
    # print(json.dumps(res, indent=4))
    return res


# ------------------------------------------------------------------------------


def update_prediction_db(preds, configpath=CONFIG_FILE_PATH):
    """update prediction results

    Arguments:
        preds {dict} -- dictionary -> {wfname: [good_prob, acdc_prob, resubmit_prob]}
        configpath {str} -- path of configs contains db connection info
    """

    if not preds: return
    config = get_yamlconfig(configpath)

    timestamp = fmttime(time.time())
    values = [
        (wf, round(predval[0], 6), round(predval[1], 6), round(predval[2], 6), timestamp)
        for wf, predval in preds.items()
    ]
    update_prediction_history_db(config, values)


# ------------------------------------------------------------------------------


def makingPredictionsWithML(docs):
    from os.path import join, dirname, abspath

    mfile = join(dirname(abspath(__file__)), "models/xgb_optimized.model")

    predres = predict_docs(docs, mfile)
    update_prediction_db(predres)


###############################################################################


def test_singleDoc(doc):
    wname, featureExtracted = extract_doc(doc)

    print()
    print(wname)
    print("-" * 79)
    print(json.dumps(featureExtracted, indent=4))
    print("-" * 79)
    print()


def test_multiDocs(docs):
    for doc in docs:
        test_singleDoc(doc)


def main():
    testdocfn = "./test/bab2ef60-b0f2-4b55-9434-95a9cfd00510.json"
    sdoc = json.load(open(testdocfn))["data"]

    print("single doc -->")
    test_singleDoc(sdoc)

    import gzip

    mdocs = []
    with gzip.GzipFile("./test/toSendDoc_190624-180839.json.gz", "r") as fin:
        mdocs = json.loads(fin.read())
    print("\n\nmultiple docs -->")
    # test_multiDocs(mdocs)

    modelfile = "./models/xgb_default.model"
    predres = predict_docs(mdocs, modelfile)
    makingPredictionsWithML(mdocs)


###############################################################################

if __name__ == "__main__":
    main()
