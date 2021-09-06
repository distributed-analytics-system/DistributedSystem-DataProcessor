'use strict';

const envalid = require('envalid');
const envVars = require('./envVars');
const { environments } = require('../constants');

const validateAndParseEnvVars = () => {
  const envVarsList = Object.keys(envVars);
  /** Prepare validators */
  const validators = envVarsList.reduce((prevState, item) => ({
    ...prevState,
    [envVars[item].name]: envVars[item].validator
  }), {});

  /** Validate and pars env vars */
  return envalid.cleanEnv(
    process.env, validators, {
      strict: false,
      transformer: cleanedEnv => envVarsList.reduce((configObj, item) => ({
        ...configObj,
        [item]: cleanedEnv[envVars[item].name]
      }), {})
    });
}

module.exports = validateAndParseEnvVars();
