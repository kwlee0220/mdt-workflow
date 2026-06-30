package mdt.workflow.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Configuration
@ConfigurationProperties(prefix = "mdt.workflow-manager.argo")
public class ArgoWorkflowManagerConfiguration {
	private String m_argoEndpoint;
	private String m_argoNamespace;
	private String m_mdtUrl;
	private String m_clientDockerImage;
	
	private String m_executionTimeEstimatorEndpoint;
	
	public String getArgoEndpoint() {
		return m_argoEndpoint;
	}
	
	public void  setArgoEndpoint(String endpoint) {
		m_argoEndpoint = endpoint;
	}
	
	public String getArgoNamespace() {
		return m_argoNamespace;
	}
	
	public void setArgoNamespace(String namespace) {
		m_argoNamespace = namespace;
	}
	
	public String getMdtUrl() {
		return m_mdtUrl;
	}
	
	public void setMdtUrl(String url) {
		m_mdtUrl = url;
	}
	
	public String getClientDockerImage() {
		return m_clientDockerImage;
	}
	
	public void setClientDockerImage(String image) {
		m_clientDockerImage = image;
	}
	
	public String getExecutionTimeEstimatorEndpoint() {
		return m_executionTimeEstimatorEndpoint;
	}
	
	public void setExecutionTimeEstimatorEndpoint(String ep) {
		m_executionTimeEstimatorEndpoint = ep;
	}
}
