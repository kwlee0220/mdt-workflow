package mdt.workflow;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.logging.DeferredLog;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import utils.io.EnvironmentFileLoader;
import utils.jdbc.JdbcUtils;
import utils.stream.KeyValueFStream;

/**
 * env 파일({@code key=value} 목록)을 읽어 Spring 환경의 가장 앞 property source({@code mdtEnv})로 넣는다.
 * <p>
 * 파일 경로는 시스템 속성 {@code env.file}, 환경 변수 {@code ENV_FILE}, 기본값 {@code config/env.file} 순으로 정한다.
 * 읽은 변수는 INFO 로그로 남기되, 키 이름에 {@code PASS}/{@code SECRET}/{@code TOKEN}/{@code CREDENTIAL}이 들어간
 * 값은 {@code ****}로, 그 밖의 값은 URL 안의 비밀번호({@code password=…}, {@code user:pw@})를 가려 출력한다.
 * 2026-11-04 이전에는 값을 그대로 출력해 비밀번호가 로그에 남았다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvironmentVariablePostProcessor implements EnvironmentPostProcessor, Ordered {
    private static final DeferredLog s_deferredLog = new DeferredLog();

    private static final String ENV_FILE_ENV_VAR = "ENV_FILE";
    private static final String ENV_FILE_PROPERTY = "env.file";
    private static final String ENV_FILE_NAME = "config/env.file";
    
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication application) {
        String fromProp = System.getProperty(ENV_FILE_PROPERTY);
        String fromEnv = System.getenv(ENV_FILE_ENV_VAR);
        String path = firstNonBlank(fromProp, fromEnv, ENV_FILE_NAME);
        boolean explicit = firstNonBlank(fromProp, fromEnv) != null;
        
        try {
            File envFile = new File(path);
            if ( !explicit && !envFile.isFile() ) {
                // 기본 경로의 파일은 없어도 된다. 2026-11-04 이전에는 매번 스택 트레이스와 함께 ERROR로 남겼다.
                s_deferredLog.info("no env file at the default location: " + envFile.getAbsolutePath());
            }
            else {
                s_deferredLog.info("trying to load environment variables from file: "
                                    + envFile.getAbsolutePath());
                EnvironmentFileLoader envLoader = EnvironmentFileLoader.from(envFile);
                LinkedHashMap<String, String> variables = envLoader.load();
                KeyValueFStream.from(variables)
                                .forEach(kv -> s_deferredLog.info(String.format("  - EnvVar: %s=%s", kv.key(),
                                                                                maskValue(kv.key(), kv.value()))));
            
                Map<String,Object> vars = KeyValueFStream.from(variables)
                                                            .mapValue(v -> (Object)v)
                                                            .toMap();
                MapPropertySource source = new MapPropertySource("mdtEnv", vars);
                env.getPropertySources().addFirst(source);
            }
        }
        catch ( IOException e ) {
            s_deferredLog.error("failed to load env file: " + path, e);
        }
        
        application.addListeners((ApplicationListener<ApplicationPreparedEvent>) event -> {
        	Log actualLogger = LogFactory.getLog(EnvironmentVariablePostProcessor.class);
			s_deferredLog.replayTo(actualLogger);
		});
    }
    
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
    
    private static final Pattern SECRET_KEY = Pattern.compile("(?i).*(PASS|SECRET|TOKEN|CREDENTIAL).*");
    
    /**
     * 로그에 출력할 변수 값을 만든다. 비밀 값으로 보이는 키는 값 전체를, 그 밖의 값은 URL 안의 비밀번호를 가린다.
     *
     * @param key	변수 이름.
     * @param value	변수 값.
     * @return	로그용 값.
     */
    static String maskValue(String key, String value) {
        if ( value == null ) {
            return null;
        }
        return SECRET_KEY.matcher(key).matches() ? "****" : JdbcUtils.maskPassword(value);
    }
    
    private static String firstNonBlank(String... arr) {
        for ( String s : arr ) {
            if ( s != null && !s.isBlank() )
                return s;
        }
        return null;
    }
}