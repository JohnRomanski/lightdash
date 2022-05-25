import {
    attachTypesToModels,
    convertExplores,
    getSchemaStructureFromDbtModels,
    isSupportedDbtAdapter,
    ParseError,
} from '@lightdash/common';
import { warehouseClientFromCredentials } from '@lightdash/warehouses';
import fetch from 'node-fetch';
import path from 'path';
import { getDbtContext } from '../dbt/context';
import { loadManifest } from '../dbt/manifest';
import { getModelsFromManifest } from '../dbt/models';
import {
    loadDbtTarget,
    warehouseCredentialsFromDbtTarget,
} from '../dbt/profile';
import * as styles from '../styles';

type GenerateHandlerOptions = {
    projectDir: string;
    profilesDir: string;
    target: string | undefined;
    profile: string | undefined;
};
export const compileHandler = async (options: GenerateHandlerOptions) => {
    const absoluteProjectPath = path.resolve(options.projectDir);
    const absoluteProfilesPath = path.resolve(options.profilesDir);
    const context = await getDbtContext({ projectDir: absoluteProjectPath });
    const profileName = options.profile || context.profileName;
    const target = await loadDbtTarget({
        profilesDir: absoluteProfilesPath,
        profileName,
        targetName: options.target,
    });
    const credentials = await warehouseCredentialsFromDbtTarget(target);
    const warehouseClient = warehouseClientFromCredentials(credentials);
    const manifest = await loadManifest({ targetDir: context.targetDir });
    const models = getModelsFromManifest(manifest);
    const catalog = await warehouseClient.getCatalog(
        getSchemaStructureFromDbtModels(models),
    );
    const typedModels = attachTypesToModels(models, catalog, true);
    if (!isSupportedDbtAdapter(manifest.metadata)) {
        throw new ParseError('');
    }
    const explores = await convertExplores(
        typedModels,
        false,
        manifest.metadata.adapter_type,
        Object.values(manifest.metrics),
    );
    console.log(`Compiled ${explores.length} explores`);
    const url = 'http://localhost:8080';
    const projectUuid = '3675b69e-8324-4110-bdca-059031aa8da3';
    const response = await fetch(
        `${url}/api/v1/projects/${projectUuid}/explores`,
        {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                Cookie: 'connect.sid=s%3A0malJ0tAY9imAK6PBS4-TKCkMJMcl6NY.vGqKixvUL5jPXEKlJ2%2FxR325RiM6dhr5HU1jLsHMlSM',
            },
            body: JSON.stringify(explores),
        },
    );
    if (response.status === 200) {
        console.log('');
        console.log(styles.success('Successfully compiled project'));
        console.log('');
        console.log(`${styles.bold('Preview your project:')}`);
        console.log('');
        console.log(
            `      ${styles.bold(
                '⚡️ http://localhost:3000/projects/3675b69e-8324-4110-bdca-059031aa8da3/tables',
            )}`,
        );
        console.log('');
        console.log('Done 🕶');
    }
};
