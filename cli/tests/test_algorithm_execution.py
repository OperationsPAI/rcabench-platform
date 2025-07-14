#!/usr/bin/env python3
"""
Test for algorithm execution functionality.
"""

from unittest.mock import Mock, patch
from rcabench_platform.v2.clients.rcabench_ import RcabenchSdkHelper
from rcabench.openapi import (
    DtoAlgorithmItem, 
    DtoExecutionPayload, 
    DtoGenericResponseDtoSubmitResp,
    DtoSubmitResp,
    DtoTrace
)


class TestAlgorithmExecution:
    """Test cases for algorithm execution functionality."""
    
    def test_execute_algorithm_basic(self):
        """Test basic algorithm execution call."""
        
        # Mock the API response
        mock_trace = DtoTrace(
            trace_id="trace-123",
            head_task_id="task-456",
            index=0
        )
        
        mock_submit_resp = DtoSubmitResp(
            group_id="group-789",
            traces=[mock_trace]
        )
        
        mock_response = DtoGenericResponseDtoSubmitResp(
            code=200,
            message="Success",
            data=mock_submit_resp
        )
        
        with patch('rcabench_platform.v2.clients.rcabench_.AlgorithmApi') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api
            mock_api.api_v1_algorithms_post.return_value = mock_response
            
            helper = RcabenchSdkHelper()
            result = helper.execute_algorithm(
                algorithm_name="test-algorithm",
                dataset_name="test-dataset"
            )
            
            # Verify API was called with correct parameters
            mock_api.api_v1_algorithms_post.assert_called_once()
            call_args = mock_api.api_v1_algorithms_post.call_args
            payload_list = call_args[1]['body']
            
            assert len(payload_list) == 1
            payload = payload_list[0]
            assert payload.algorithm.name == "test-algorithm"
            assert payload.dataset == "test-dataset"
            
            # Verify response
            assert result.code == 200
            assert result.message == "Success"
            assert result.data.group_id == "group-789"
            assert len(result.data.traces) == 1
            assert result.data.traces[0].trace_id == "trace-123"
    
    def test_execute_algorithm_with_image_and_tag(self):
        """Test algorithm execution with custom image and tag."""
        
        mock_response = DtoGenericResponseDtoSubmitResp(
            code=200,
            message="Success",
            data=DtoSubmitResp(group_id="group-123", traces=[])
        )
        
        with patch('rcabench_platform.v2.clients.rcabench_.AlgorithmApi') as mock_api_class:
            mock_api = Mock()
            mock_api_class.return_value = mock_api
            mock_api.api_v1_algorithms_post.return_value = mock_response
            
            helper = RcabenchSdkHelper()
            result = helper.execute_algorithm(
                algorithm_name="custom-algorithm",
                dataset_name="custom-dataset",
                image="custom-image",
                tag="v1.0"
            )
            
            # Verify API was called with correct parameters
            call_args = mock_api.api_v1_algorithms_post.call_args
            payload_list = call_args[1]['body']
            payload = payload_list[0]
            
            assert payload.algorithm.name == "custom-algorithm"
            assert payload.algorithm.image == "custom-image"
            assert payload.algorithm.tag == "v1.0"
            assert payload.dataset == "custom-dataset"
    
    def test_dto_models_creation(self):
        """Test that DTO models can be created correctly."""
        
        # Test DtoAlgorithmItem creation
        algo_item = DtoAlgorithmItem(name="test-algo")
        assert algo_item.name == "test-algo"
        assert algo_item.image is None
        assert algo_item.tag is None
        
        # Test DtoAlgorithmItem with image and tag
        algo_item_full = DtoAlgorithmItem(
            name="test-algo", 
            image="test-image", 
            tag="test-tag"
        )
        assert algo_item_full.name == "test-algo"
        assert algo_item_full.image == "test-image"
        assert algo_item_full.tag == "test-tag"
        
        # Test DtoExecutionPayload creation
        payload = DtoExecutionPayload(
            algorithm=algo_item,
            dataset="test-dataset"
        )
        assert payload.algorithm.name == "test-algo"
        assert payload.dataset == "test-dataset"


if __name__ == "__main__":
    # Run tests if executed directly
    test = TestAlgorithmExecution()
    test.test_execute_algorithm_basic()
    test.test_execute_algorithm_with_image_and_tag()
    test.test_dto_models_creation()
    print("All tests passed!")